#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Download DBIS-Daten des DLA"""

import argparse
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

__author__ = 'Felix Lohmeier'

API_BASE = 'https://dbis-api.ur.de/api/v1'
SUBJECTS = [4, 9, 16, 25, 31]


def fetch_json(url: str):
    """Lade ein JSON-Dokument von der DBIS-API."""
    try:
        with urlopen(url, timeout=30) as response:
            return json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f'HTTP-Fehler bei {url}: {exc.code} {exc.reason}') from exc
    except URLError as exc:
        raise RuntimeError(f'Netzwerkfehler bei {url}: {exc.reason}') from exc


def fetch_ids_for_subject(subject_id: int) -> list[int]:
    url = (
        f'{API_BASE}/resourceIdsBySubject/{subject_id}/organization/DLA'
        '?include_unlicensed=false'
    )
    data = fetch_json(url)
    if not isinstance(data, list):
        raise RuntimeError(f'Unerwartetes Antwortformat fuer Subject {subject_id}: {type(data)}')
    return [int(item) for item in data]


def fetch_record(record_id: int) -> dict:
    url = f'{API_BASE}/resource/{record_id}/organization/DLA?language=de'
    data = fetch_json(url)
    if not isinstance(data, dict):
        raise RuntimeError(f'Unerwartetes Antwortformat fuer Datensatz {record_id}: {type(data)}')
    return data


def collect_unique_ids(subjects: list[int]) -> list[int]:
    all_ids = []
    for subject_id in subjects:
        ids = fetch_ids_for_subject(subject_id)
        print(f'Subject {subject_id}: {len(ids)} IDs geladen')
        all_ids.extend(ids)

    # Reihenfolge stabil halten und Duplikate entfernen.
    unique_ids = list(dict.fromkeys(all_ids))
    print(f'Insgesamt {len(all_ids)} IDs, davon {len(unique_ids)} eindeutig')
    return unique_ids


def write_jsonl(record_ids: list[int], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open('w', encoding='utf-8') as outfile:
        for index, record_id in enumerate(record_ids, start=1):
            record = fetch_record(record_id)
            outfile.write(json.dumps(record, ensure_ascii=False) + '\n')
            if index % 50 == 0 or index == len(record_ids):
                print(f'{index}/{len(record_ids)} Datensaetze geladen')


parser = argparse.ArgumentParser()
parser.add_argument('-o', '--output', help='Output file name', default='input/dbis-dla.jsonl')
args = parser.parse_args()

output_path = Path(args.output)
record_ids = collect_unique_ids(SUBJECTS)
write_jsonl(record_ids, output_path)
print(f'JSONL geschrieben nach {output_path}')
