"""
One-time/periodic conversion tool: reads a SQLite copy of the EVE SDE and
writes a flat JSON snapshot (regions + station->system->region mapping) that
aggloader-esi.py reads at runtime instead of touching any SQL database.

Only needs re-running when the SDE itself changes (new stations/systems are
rare - CCP expansions, not routine). Not part of the scheduled pipeline.
"""
import argparse
import datetime
import json
import sqlite3


def build_snapshot(sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    try:
        cur = conn.execute(
            'select stationID, solarSystemID, regionID from staStations where stationID<100000000'
        )
        stations = {
            str(station_id): {'solarSystemID': solar_system_id, 'regionID': region_id}
            for station_id, solar_system_id, region_id in cur.fetchall()
        }

        # Every solar system's regionID, not just ones with an NPC station -
        # structurePoller.py needs this to resolve a citadel's regionID, since
        # a citadel can be anchored in a system with no station of its own.
        cur = conn.execute('select solarSystemID, regionID from mapSolarSystems')
        solar_systems = {str(solar_system_id): region_id for solar_system_id, region_id in cur.fetchall()}
    finally:
        conn.close()

    regions = sorted({info['regionID'] for info in stations.values()})

    return {
        'generated_at': datetime.datetime.now(datetime.UTC).isoformat(),
        'regions': regions,
        'stations': stations,
        'solarSystems': solar_systems,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('sqlite_path', help='path to the SQLite SDE file')
    parser.add_argument('--out', default='sde_snapshot.json', help='output path (default: sde_snapshot.json)')
    args = parser.parse_args()

    snapshot = build_snapshot(args.sqlite_path)
    with open(args.out, 'w') as f:
        json.dump(snapshot, f)

    print("Wrote {} regions, {} stations and {} solar systems to {}".format(
        len(snapshot['regions']), len(snapshot['stations']), len(snapshot['solarSystems']), args.out))


if __name__ == "__main__":
    main()
