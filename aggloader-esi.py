import argparse
import base64
import configparser
import csv
import datetime
import glob
import io
import json
import logging
import os
import sys
import time
import traceback
from concurrent.futures import as_completed

import numpy
import pandas
import redis
import requests
from requests_futures.sessions import FuturesSession

REQUEST_TIMEOUT = 30
LOG_RETENTION_DAYS = 7
REDIS_TTL_SECONDS = 5400
# Fixed for now so this can be pointed at a shared/production Redis instance
# during testing without colliding with the real production keys.
REDIS_KEY_PREFIX = "cli-"

ORDER_COLUMNS = ['orderID', 'typeID', 'issued', 'buy', 'volume', 'volumeEntered',
                  'minVolume', 'price', 'stationID', 'range', 'duration', 'region']


def setup_logging(debug=False):
    today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    logfile = 'logs/aggloader-esi-{}.log'.format(today)
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(filename=logfile, level=level, format='%(asctime)s %(levelname)s %(message)s')

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=LOG_RETENTION_DAYS)
    for path in glob.glob('logs/aggloader-esi-*.log'):
        datestr = os.path.basename(path)[len('aggloader-esi-'):-len('.log')]
        try:
            filedate = datetime.datetime.strptime(datestr, '%Y-%m-%d')
        except ValueError:
            continue
        if filedate < cutoff:
            os.remove(path)


def get_data(requests_connection, url, retry, page, region, structure):
    future = requests_connection.get(url + str(page), timeout=REQUEST_TIMEOUT)
    logging.info('getting {}#{}#{}#{}'.format(retry, page, region, url + str(page)))
    future.url = url
    future.fullurl = url + str(page)
    future.page = page
    future.retry = retry
    future.region = region
    future.structure = structure
    return future


def process_data(result, orderwriter):
    try:
        resp = result.result()
        logging.info('Process {} {} {} {}'.format(resp.status_code, result.url, result.retry, result.region))
        if resp.status_code == 200:
            try:
                orders = resp.json()
                logging.info('{} orders on page {} {}'.format(len(orders), result.fullurl, result.page))
                for order in orders:
                    if not result.structure and int(order['location_id']) > 100000000 and order['is_buy_order']:
                        pass
                    else:
                        orderwriter.writerow([order['order_id'],
                                               order['type_id'],
                                               order['issued'],
                                               order['is_buy_order'],
                                               order['volume_remain'],
                                               order['volume_total'],
                                               order['min_volume'],
                                               order['price'],
                                               order['location_id'],
                                               order['range'],
                                               order['duration'],
                                               result.region])

                if len(orders) > 0:
                    if int(result.page) < int(resp.headers['X-Pages']):
                        logging.info('{}'.format(resp.headers['X-Pages']))
                        nextpage = result.url
                    else:
                        nextpage = None
                else:
                    nextpage = None
                logging.info('{}: next page {}'.format(result.url, nextpage))
                return {'retry': 0, 'url': nextpage, 'region': result.region, 'page': result.page + 1, 'structure': result.structure}
            except Exception as inst:
                logging.error("URL: {} could not be parsed".format(result.url))
                logging.error("{} {} {} {}".format(type(inst), inst.args, inst, traceback.format_exc()))
                with open("logs/{}-{}.txt".format(result.region, result.page), "wb") as f:
                    f.write(resp.content)
        elif resp.status_code == 403:
            logging.error("403 status. {} returned {}".format(resp.url, resp.status_code))
            return {'retry': 4}
        elif resp.status_code == 404:
            logging.error("404 status. {} returned {}".format(resp.url, resp.status_code))
            return {'retry': 4}
        elif resp.status_code == 420:
            logging.error("420 status. sleeping for 60.  {} returned {} on retry {}".format(resp.url, resp.status_code, result.retry))
            time.sleep(60)
            return {'retry': result.retry + 1, 'url': result.url, 'region': result.region, 'page': result.page, 'structure': result.structure}
        else:
            logging.error("Non 200 status. {} returned {} on retry {}".format(resp.url, resp.status_code, result.retry))
            return {'retry': result.retry + 1, 'url': result.url, 'region': result.region, 'page': result.page, 'structure': result.structure}
    except requests.exceptions.ConnectionError as e:
        logging.error(e)
        return {'retry': result.retry + 1, 'url': result.url, 'region': result.region, 'page': result.page, 'structure': result.structure}
    return {'retry': result.retry + 1, 'url': result.url, 'region': result.region, 'page': result.page, 'structure': result.structure}


def fetch_wave(session, urls, orderwriter, requeue_retries):
    # requeue_retries mirrors the (slightly asymmetric) requeue behaviour of
    # the production script: the region-order wave retries on {1,2}, the
    # structure-order wave only on {1}.
    while len(urls) > 0:
        futures = []
        logging.warning("Loop restarting")
        for url in urls:
            logging.info('URL:{}  Retry:{} page:{}'.format(url['url'], url['retry'], url['page']))
            futures.append(get_data(session, url['url'], url['retry'], url['page'], url['region'], url['structure']))
        urls = []
        for result in as_completed(futures):
            presult = process_data(result, orderwriter)
            if presult['retry'] in requeue_retries:
                urls.append(presult)
                logging.info("adding {} to retry {}".format(result.url, presult['retry']))
            if presult['retry'] == 0 and presult['url'] is not None:
                logging.info('{} has more pages. {}'.format(result.url, presult['retry']))
                urls.append(presult)


def load_sde_snapshot(path):
    # Built ahead of time by build_sde_snapshot.py from a SQLite SDE copy -
    # this script itself never touches any SQL database.
    with open(path) as f:
        data = json.load(f)
    regionids = data['regions']
    stations_df = pandas.DataFrame([
        {'stationID': int(station_id), 'solarSystemID': info['solarSystemID']}
        for station_id, info in data['stations'].items()
    ])
    return regionids, stations_df


def load_structures(path):
    # Maintained by structurePoller.py on its own schedule. Absent/empty is a
    # valid state (no structures discovered yet), not an error.
    if not os.path.exists(path):
        return pandas.DataFrame(columns=['stationID', 'regionID', 'solarSystemID'])
    with open(path) as f:
        data = json.load(f)
    if not data['structures']:
        return pandas.DataFrame(columns=['stationID', 'regionID', 'solarSystemID'])
    return pandas.DataFrame(data['structures'])[['stationID', 'regionID', 'solarSystemID']]


def compute_side(df, is_buy):
    if is_buy:
        extreme = df.groupby('what')['price'].transform('max')
        df['volume'] = df['volume'].where(df['price'] >= extreme / 100, 0)
    else:
        extreme = df.groupby('what')['price'].transform('min')
        df['volume'] = df['volume'].where(df['price'] <= extreme * 100, 0)
    df['cumsum'] = df.groupby('what')['volume'].cumsum()
    df['fivepercent'] = df.groupby('what')['volume'].transform('sum') / 20
    df['lastsum'] = df.groupby('what')['cumsum'].shift(1)
    df.fillna(0, inplace=True)
    df['applies'] = numpy.where(df['cumsum'] <= df['fivepercent'], df['volume'], df['fivepercent'] - df['lastsum'])
    num = df._get_numeric_data()
    num[num < 0] = 0
    df['applies'] = df['applies'].mask(df.groupby('what')['applies'].transform('sum') == 0, 0.01)
    df['weight'] = df['volume'].mask(df.groupby('what')['volume'].transform('sum') == 0, 0.01)

    df['_wp'] = df['price'] * df['weight']
    df['_wp5'] = df['price'] * df['applies']
    g = df.groupby('what')
    agg = pandas.DataFrame()
    agg['weightedaverage'] = g['_wp'].sum() / g['weight'].sum()
    agg['maxval'] = g['price'].max()
    agg['minval'] = g['price'].min()
    agg['stddev'] = g['price'].std()
    agg['median'] = g['price'].median()
    agg.fillna(0.01, inplace=True)
    agg['volume'] = g['volume'].sum()
    agg['numorders'] = g['price'].count()
    agg['fivepercent'] = g['_wp5'].sum() / g['applies'].sum()
    return agg


def make_what(df, scope_col, literal_scope):
    # checkFreshness.py and the production keys both use lowercase
    # 'true'/'false' (Postgres's boolean-to-text cast) - plain str(bool)
    # would produce 'True'/'False' and silently break key matching.
    buy_str = df['buy'].map({True: 'true', False: 'false'})
    scope_part = literal_scope if literal_scope is not None else df[scope_col].astype(str)
    return scope_part + '|' + df['typeID'].astype(str) + '|' + buy_str


def run_scope_pass(orders_df, scope_col, pipe, literal_scope=None):
    group_cols = ([scope_col] if literal_scope is None else []) + ['typeID', 'buy', 'price']
    price_levels = orders_df.groupby(group_cols, as_index=False)['volume'].sum()
    price_levels['what'] = make_what(price_levels, scope_col, literal_scope)

    sell = price_levels[~price_levels['buy']].sort_values(['what', 'price'], ascending=True).reset_index(drop=True)
    buy = price_levels[price_levels['buy']].sort_values(['what', 'price'], ascending=False).reset_index(drop=True)

    sellagg = compute_side(sell, is_buy=False)
    buyagg = compute_side(buy, is_buy=True)
    agg2 = pandas.concat([buyagg, sellagg])

    write_redis(agg2, pipe)
    return agg2


def write_redis(agg2, pipe):
    count = 0
    for row in agg2.itertuples():
        pipe.set(REDIS_KEY_PREFIX + row[0],
                  "{}|{}|{}|{}|{}|{}|{}|{}".format(row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]),
                  ex=REDIS_TTL_SECONDS)
        count += 1
        if count > 1000:
            count = 0
            pipe.execute()
    pipe.execute()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--regions', help='comma-separated regionIDs to limit the fetch to (testing only)')
    args = parser.parse_args()

    setup_logging(args.debug)

    file_location = os.path.dirname(os.path.realpath(__file__))
    inifile = os.path.join(file_location, 'config.cfg')

    config = configparser.ConfigParser()
    config.read(inifile)

    clientid = config.get('oauth', 'clientid')
    secret = config.get('oauth', 'secret')
    refreshtoken = config.get('oauth', 'refreshtoken')

    reqs_num_workers = config.getint('requests', 'max_workers')
    useragent = config.get('requests', 'useragent')

    sde_snapshot_path = config.get('data', 'sde_snapshot', fallback='sde_snapshot.json')
    structures_path = config.get('data', 'structures', fallback='structures.json')

    redis_host = config.get('redis', 'host', fallback='localhost')
    redis_port = config.getint('redis', 'port', fallback=6379)
    redis_db = config.getint('redis', 'db', fallback=0)
    redis_password = config.get('redis', 'password', fallback=None)

    compatdate = (datetime.datetime.utcnow() - datetime.timedelta(hours=11)).strftime('%Y-%m-%d')

    session = FuturesSession(max_workers=reqs_num_workers)
    session.headers.update({'User-Agent': useragent, 'X-Compatibility-Date': compatdate})

    logging.warning("Loading SDE snapshot from {}".format(sde_snapshot_path))
    regionids, stations_df = load_sde_snapshot(sde_snapshot_path)
    logging.warning("Loading structures from {}".format(structures_path))
    structures_df = load_structures(structures_path)

    wanted_regions = None
    if args.regions:
        wanted_regions = {int(r) for r in args.regions.split(',')}
        regionids = [r for r in regionids if r in wanted_regions]
        structures_df = structures_df[structures_df['regionID'].isin(wanted_regions)]

    urls = []
    for regionid in regionids:
        urls.append({'url': "https://esi.evetech.net/markets/{}/orders/?order_type=all&datasource=tranquility&page=".format(regionid),
                      'retry': 0, 'page': 1, 'region': regionid, 'structure': 0})

    # Virtual PLEX Market region - not a real space region, so it has no
    # entry in staStations and gets skipped above unless added explicitly.
    if wanted_regions is None or 19000001 in wanted_regions:
        urls.append({'url': "https://esi.evetech.net/markets/19000001/orders/?order_type=all&datasource=tranquility&page=",
                      'retry': 0, 'page': 1, 'region': 19000001, 'structure': 0})

    buf = io.StringIO()
    orderwriter = csv.writer(buf, quoting=csv.QUOTE_MINIMAL, delimiter="\t")

    logging.warning("Fetching region orders")
    fetch_wave(session, urls, orderwriter, requeue_retries={1, 2})

    if structures_df.empty:
        # Nothing to fetch, so skip authenticating entirely -- lets this run
        # before structurePoller.py has ever populated structures.json.
        logging.warning("No structures known, skipping OAuth and structure-order fetch")
    else:
        # Structure orders need an authenticated (OAuth) ESI call.
        token = clientid + ':' + secret
        message_bytes = token.encode('ascii')
        base64_message = base64.b64encode(message_bytes).decode('ascii')
        headers = {'Authorization': 'Basic ' + base64_message, 'User-Agent': useragent,
                   "Content-Type": "application/x-www-form-urlencoded"}
        query = {'grant_type': 'refresh_token', 'refresh_token': refreshtoken}
        r = requests.post('https://login.eveonline.com/v2/oauth/token', data=query, headers=headers, timeout=REQUEST_TIMEOUT)
        response = r.json()
        accesstoken = response['access_token']
        refreshtokennew = response['refresh_token']
        if refreshtokennew != refreshtoken:
            with open(inifile, 'w') as cfgfile:
                config.set('oauth', 'refreshtoken', refreshtokennew)
                config.write(cfgfile)

        logging.debug("Access Token {}".format(accesstoken))
        logging.debug("refresh Token {}".format(refreshtokennew))

        session.headers.update({'Authorization': 'Bearer ' + accesstoken, 'X-Compatibility-Date': compatdate})

        urls = []
        for _, row in structures_df.iterrows():
            urls.append({'url': "https://esi.evetech.net/markets/structures/{}/?&datasource=tranquility&page=".format(row['stationID']),
                          'retry': 0, 'page': 1, 'region': row['regionID'], 'structure': 1})

        logging.warning("Fetching structure orders")
        fetch_wave(session, urls, orderwriter, requeue_retries={1})

    buf.seek(0)
    orders_df = pandas.read_csv(buf, sep="\t", header=None, names=ORDER_COLUMNS)
    # csv.writer renders Python bools as the literal strings 'True'/'False'
    # regardless of whether read_csv already inferred a bool dtype for them.
    orders_df['buy'] = orders_df['buy'].astype(str).map({'True': True, 'False': False})
    logging.warning("Fetched {} order rows".format(len(orders_df)))

    redisdb = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)
    pipe = redisdb.pipeline()

    logging.warning("Region Aggregates")
    run_scope_pass(orders_df, 'region', pipe)

    logging.warning("Station Aggregates")
    run_scope_pass(orders_df, 'stationID', pipe)

    logging.warning("System Aggregates")
    all_stations_df = pandas.concat([
        stations_df[['stationID', 'solarSystemID']],
        structures_df[['stationID', 'solarSystemID']],
    ], ignore_index=True)
    system_df = orders_df.merge(all_stations_df, on='stationID', how='inner')
    run_scope_pass(system_df, 'solarSystemID', pipe)

    logging.warning("Universe Aggregates")
    run_scope_pass(orders_df, None, pipe, literal_scope='0')

    redisdb.set(REDIS_KEY_PREFIX + "fp-lastupdate", datetime.datetime.utcnow().isoformat())

    logging.warning("Complete")


if __name__ == "__main__":
    main()
