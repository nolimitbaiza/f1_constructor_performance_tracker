"""Build the race dashboard snapshot and optionally upsert it into PostgreSQL."""
import argparse
import csv
import json
import math
import os
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read_csv(directory, name):
    with (directory / f"{name}.csv").open(encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def build(directory):
    constructors = [dict(id=int(r['constructorId']), ref=r['constructorRef'], name=r['name'], nationality=r['nationality']) for r in read_csv(directory, 'constructors')]
    races = {int(r['raceId']): dict(id=int(r['raceId']), season=int(r['year']), round=int(r['round']), name=r['name'], date=r['date']) for r in read_csv(directory, 'races') if int(r['year']) >= 1958}
    results = {}
    for r in read_csv(directory, 'results'):
        race, constructor = int(r['raceId']), int(r['constructorId'])
        if race not in races:
            continue
        key = (race, constructor)
        item = results.setdefault(key, dict(raceId=race, constructorId=constructor, wins=0, podiums=0, entries=0, classified=0, finishSum=0, poles=0, racePoints=0, sprintPoints=0, championshipPoints=None, rank=None))
        position = int(r['position']) if r['position'] != '\\N' else None
        item['entries'] += 1
        item['wins'] += position == 1
        item['podiums'] += position is not None and position <= 3
        item['poles'] += r['grid'] == '1'
        if position is not None:
            item['classified'] += 1
            item['finishSum'] += position
    for r in read_csv(directory, 'constructor_results'):
        key = (int(r['raceId']), int(r['constructorId']))
        if key in results:
            results[key]['racePoints'] = float(r['points'])
    for r in read_csv(directory, 'sprint_results'):
        key = (int(r['raceId']), int(r['constructorId']))
        if key in results:
            results[key]['sprintPoints'] += float(r['points'])
    for item in results.values():
        item['racePoints'] -= item['sprintPoints']
    standings = []
    for r in read_csv(directory, 'constructor_standings'):
        race, constructor = int(r['raceId']), int(r['constructorId'])
        if race in races:
            standings.append(dict(raceId=race, constructorId=constructor, points=0.0 if r['positionText'] == 'E' else float(r['points']), rank=int(r['position']), excluded=r['positionText'] == 'E'))
    completed = {r['raceId'] for r in results.values()}
    race_list = sorted((r for r in races.values() if r['id'] in completed), key=lambda r: (r['season'], r['round']))
    for s in standings:
        key = (s['raceId'], s['constructorId'])
        if key in results:
            results[key]['championshipPoints'] = s['points']
            results[key]['rank'] = s['rank']
    scheduled = defaultdict(int)
    for r in races.values():
        scheduled[r['season']] += 1
    seasons = [dict(year=year, races=sum(r['season'] == year for r in race_list), scheduled=scheduled[year], complete=sum(r['season'] == year for r in race_list) == scheduled[year]) for year in sorted({r['season'] for r in race_list})]
    data = dict(meta=dict(source='Repository race results and official constructor standings', through=max(r['date'] for r in race_list)), constructors=constructors, seasons=seasons, races=race_list, results=list(results.values()), standings=[s for s in standings if s['raceId'] in completed])
    validate(data)
    return data


def validate(data):
    constructors = {r['id'] for r in data['constructors']}
    races = {r['id'] for r in data['races']}
    seen = set()
    for row in data['results']:
        key = (row['raceId'], row['constructorId'])
        if key in seen or key[0] not in races or key[1] not in constructors:
            raise ValueError(f'Invalid or duplicate race-constructor key: {key}')
        seen.add(key)
        if any(not math.isfinite(row[field]) for field in ('racePoints', 'sprintPoints', 'finishSum')):
            raise ValueError(f'Non-finite result: {key}')
        if not 0 <= row['wins'] <= row['podiums'] <= row['entries'] or not 0 <= row['classified'] <= row['entries']:
            raise ValueError(f'Invalid result counts: {key}')

    seen_standings = set()
    for row in data['standings']:
        key = (row['raceId'], row['constructorId'])
        if key in seen_standings or key[0] not in races or key[1] not in constructors or not math.isfinite(row['points']) or row['rank'] < 1:
            raise ValueError(f'Invalid standing: {key}')
        seen_standings.add(key)


def load_postgres(data, url):
    import psycopg
    from psycopg.types.json import Jsonb
    with psycopg.connect(url) as conn:
        conn.execute((ROOT / 'database/schema.sql').read_text())
        for table, rows, keys in [('constructors', data['constructors'], ['id']), ('seasons', data['seasons'], ['year']), ('races', data['races'], ['id']), ('results', data['results'], ['raceId', 'constructorId']), ('standings', data['standings'], ['raceId', 'constructorId'])]:
            columns = list(rows[0])
            quoted = ','.join(f'"{c}"' for c in columns)
            updates = ','.join(f'"{c}"=EXCLUDED."{c}"' for c in columns if c not in keys)
            conflict = ','.join(f'"{c}"' for c in keys)
            query = f'INSERT INTO f1.{table} ({quoted}) VALUES ({",".join(["%s"] * len(columns))}) ON CONFLICT ({conflict}) DO UPDATE SET {updates}'
            with conn.cursor() as cursor:
                cursor.executemany(query, [tuple(row[c] for c in columns) for row in rows])
        conn.execute("INSERT INTO f1.metadata (id, data) VALUES (1, %s) ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data", [Jsonb(data['meta'])])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--raw-dir', type=Path, default=ROOT / 'data/raw')
    parser.add_argument('--output', type=Path, default=ROOT / 'docs/data.json')
    parser.add_argument('--database', action='store_true', help='Also upsert into DATABASE_URL')
    args = parser.parse_args()
    data = build(args.raw_dir)
    if args.database:
        if not os.getenv('DATABASE_URL'):
            parser.error('--database requires DATABASE_URL')
        load_postgres(data, os.environ['DATABASE_URL'])
    temporary = args.output.with_suffix('.tmp')
    temporary.write_text(json.dumps(data, separators=(',', ':'), allow_nan=False))
    temporary.replace(args.output)
    print(f"Built {len(data['results'])} constructor results through {data['meta']['through']}")


if __name__ == '__main__':
    main()
