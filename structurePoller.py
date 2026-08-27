"""
Discovers player-owned structures (citadels) with a public market and writes
their {stationID, regionID, solarSystemID} to a flat structures.json file --
no database, no Redis. Mirrors production's getStations/checkCitadels.py
logic (a region-wide buy-order sweep to spot location_id>1e8, plus ESI's
public "structures with a market" listing), but resolves against the flat
sde_snapshot.json for the region list instead of Postgres, and never writes
anywhere but this one JSON file.

Meant to run on its own schedule (e.g. daily, like production's
updateCitadels.sh cron) via structurePoller.sh -- separate from loadPrices.sh,
since structure discovery only needs to catch up with newly
anchored/unanchored citadels, not run every load cycle.
"""
import argparse
import base64
import configparser
import datetime
import glob
import json
import logging
import os
import time
from concurrent.futures import as_completed

import requests
from requests_futures.sessions import FuturesSession

REQUEST_TIMEOUT = 30
LOG_RETENTION_DAYS = 7
STRUCTURE_ID_THRESHOLD = 100000000


def setup_logging(debug=False):
    today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    logfile = 'logs/structurePoller-{}.log'.format(today)
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(filename=logfile, level=level, format='%(asctime)s %(levelname)s %(message)s')

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=LOG_RETENTION_DAYS)
    for path in glob.glob('logs/structurePoller-*.log'):
        datestr = os.path.basename(path)[len('structurePoller-'):-len('.log')]
        try:
            filedate = datetime.datetime.strptime(datestr, '%Y-%m-%d')
        except ValueError:
            continue
        if filedate < cutoff:
            os.remove(path)


def get_data(requests_connection, url, retry, page, region):
    future = requests_connection.get(url + str(page), timeout=REQUEST_TIMEOUT)
    future.url = url
    future.page = page
    future.retry = retry
    future.region = region
    return future


def process_buy_orders(result, discovered):
    try:
        resp = result.result()
        if resp.status_code == 200:
            orders = resp.json()
            for order in orders:
                if int(order['location_id']) > STRUCTURE_ID_THRESHOLD:
                    discovered.add(int(order['location_id']))
            if len(orders) > 0 and int(result.page) < int(resp.headers['X-Pages']):
                nextpage = result.url
            else:
                nextpage = None
            return {'retry': 0, 'url': nextpage, 'region': result.region, 'page': result.page + 1}
        elif resp.status_code in (403, 404):
            logging.error("{} status. {} returned {}".format(resp.status_code, resp.url, resp.status_code))
            return {'retry': 4}
        elif resp.status_code == 420:
            logging.error("420 status, sleeping 60. {}".format(resp.url))
            time.sleep(60)
            return {'retry': result.retry + 1, 'url': result.url, 'region': result.region, 'page': result.page}
        else:
            logging.error("Non 200 status {} for {}".format(resp.status_code, resp.url))
            return {'retry': result.retry + 1, 'url': result.url, 'region': result.region, 'page': result.page}
    except requests.exceptions.ConnectionError as e:
        logging.error(e)
        return {'retry': result.retry + 1, 'url': result.url, 'region': result.region, 'page': result.page}


def discover_from_buy_orders(session, regionids):
    urls = [{'url': "https://esi.evetech.net/markets/{}/orders/?order_type=buy&datasource=tranquility&page=".format(r),
             'retry': 0, 'page': 1, 'region': r} for r in regionids]
    discovered = set()
    while urls:
        futures = [get_data(session, u['url'], u['retry'], u['page'], u['region']) for u in urls]
        urls = []
        for result in as_completed(futures):
            presult = process_buy_orders(result, discovered)
            if presult is None:
                continue
            if presult['retry'] in (1, 2) or (presult['retry'] == 0 and presult['url'] is not None):
                urls.append(presult)
    return discovered


def discover_from_public_listing(session):
    resp = session.get('https://esi.evetech.net/latest/universe/structures/?datasource=tranquility&filter=market',
                        timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return set(resp.json())


def resolve_structure(session, structure_id):
    resp = session.get("https://esi.evetech.net/latest/universe/structures/{}/".format(structure_id),
                        timeout=REQUEST_TIMEOUT)
    if resp.status_code == 200:
        return resp.json()
    logging.warning("Structure {} failed to resolve, status {}".format(structure_id, resp.status_code))
    return None


def get_access_token(config, inifile):
    clientid = config.get('oauth', 'clientid')
    secret = config.get('oauth', 'secret')
    refreshtoken = config.get('oauth', 'refreshtoken')
    useragent = config.get('requests', 'useragent')

    token = clientid + ':' + secret
    base64_message = base64.b64encode(token.encode('ascii')).decode('ascii')
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
    return accesstoken


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--regions', help='comma-separated regionIDs to limit the buy-order sweep to (testing only)')
    args = parser.parse_args()

    setup_logging(args.debug)

    file_location = os.path.dirname(os.path.realpath(__file__))
    inifile = os.path.join(file_location, 'structurePoller.cfg')
    config = configparser.ConfigParser()
    config.read(inifile)

    reqs_num_workers = config.getint('requests', 'max_workers')
    useragent = config.get('requests', 'useragent')
    sde_snapshot_path = config.get('data', 'sde_snapshot', fallback='sde_snapshot.json')
    out_path = config.get('data', 'out', fallback='structures.json')

    with open(sde_snapshot_path) as f:
        sde = json.load(f)
    regionids = sde['regions']
    solar_systems = sde['solarSystems']

    if args.regions:
        wanted = {int(r) for r in args.regions.split(',')}
        regionids = [r for r in regionids if r in wanted]

    logging.warning("Authenticating")
    accesstoken = get_access_token(config, inifile)

    session = FuturesSession(max_workers=reqs_num_workers)
    session.headers.update({'User-Agent': useragent, 'Authorization': 'Bearer ' + accesstoken})

    rsession = requests.Session()
    rsession.headers.update({'User-Agent': useragent, 'Authorization': 'Bearer ' + accesstoken})

    logging.warning("Sweeping buy orders across {} regions for structure IDs".format(len(regionids)))
    candidates = discover_from_buy_orders(session, regionids)
    logging.warning("Found {} candidate structure IDs in buy orders".format(len(candidates)))

    logging.warning("Fetching public structures-with-market listing")
    candidates |= discover_from_public_listing(rsession)
    logging.warning("{} total candidate structure IDs".format(len(candidates)))

    structures = []
    for structure_id in candidates:
        details = resolve_structure(rsession, structure_id)
        if details is None:
            continue
        solar_system_id = details['solar_system_id']
        region_id = solar_systems.get(str(solar_system_id))
        if region_id is None:
            logging.warning("Structure {} in unknown solar system {}, skipping (SDE snapshot may be stale)"
                             .format(structure_id, solar_system_id))
            continue
        structures.append({
            'stationID': structure_id,
            'stationName': details.get('name'),
            'solarSystemID': solar_system_id,
            'regionID': region_id,
        })

    logging.warning("Resolved {} of {} candidate structures".format(len(structures), len(candidates)))

    snapshot = {
        'generated_at': datetime.datetime.utcnow().isoformat(),
        'structures': structures,
    }
    with open(out_path, 'w') as f:
        json.dump(snapshot, f)

    logging.warning("Wrote {} structures to {}".format(len(structures), out_path))


if __name__ == "__main__":
    main()
