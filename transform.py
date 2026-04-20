#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Transform DBIS-Daten des DLA"""

import argparse
import json
import pandas as pd

__author__ = 'Felix Lohmeier'

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', help='Input file name', default='input/dbis-dla.jsonl')
parser.add_argument('-o', '--output', help='Output file name', default='output/dbis-dla.tsv')
args = parser.parse_args()

df = pd.read_json(args.input, lines=True)

df_out = pd.DataFrame(index=df.index)

df_out['source'] = 'DBIS'
df_out['filterSource'] = 'Digitale Nachschlagewerke'
df_out['category'] = 'Bibliotheksdokumente'
df_out['categorySub'] = 'Datenbankressource'
df_out['id'] = df['id'].astype('string')
df_out['display'] = df['title'].fillna('')
df_out['title'] = df['title'].fillna('')
df_out['titleMain_text'] = df['title'].fillna('')
df_out['displayAddition1'] = df['description_short'].fillna('')
df_out['displayAddition2'] = df['types'].apply(
	lambda types: ', '.join([type_entry.get('title', '') for type_entry in types if type_entry.get('title')])
)
df_out['description_text_mv'] = df['description'].fillna('')
df_out['note'] = df['note'].fillna('')
df_out['usageRestrictionNote'] = df['instructions'].fillna('')
df_out['confidential'] = False
df_out['filterDigital'] = df['licenses'].apply(
	lambda licenses: bool(licenses) and any(
		bool(access.get('accessUrl'))
		for license_entry in licenses
		for access in license_entry.get('accesses', [])
	)
)
df_out['dateCataloged'] = pd.to_datetime(df['created_at'], errors='coerce').dt.date.astype('string')
df_out['dateModified'] = pd.to_datetime(df['modified_at'], errors='coerce').dt.date.astype('string')
df_out['dateOrigin'] = pd.to_datetime(df['publication_time_start'], errors='coerce').dt.year.astype('Int64').astype('string')
df_out['isbn_mv'] = df['isbn_issn'].fillna('').astype('string').str.strip()
df_out['creator_display_mv'] = df['authors'].apply(
	lambda authors: json.dumps([author.get('title', '') for author in authors], ensure_ascii=False)
)
df_out['creator_id_mv'] = df['authors'].apply(
	lambda authors: json.dumps([str(author.get('id', '')) for author in authors], ensure_ascii=False)
)
df_out['subject_display_mv'] = df['subjects'].apply(
	lambda subjects: json.dumps([subject.get('title', '') for subject in subjects], ensure_ascii=False)
)
df_out['subject_id_mv'] = df['subjects'].apply(
	lambda subjects: json.dumps([str(subject.get('id', '')) for subject in subjects], ensure_ascii=False)
)
df_out['filterSubject_mv'] = df['subjects'].apply(
	lambda subjects: json.dumps([subject.get('title', '') for subject in subjects], ensure_ascii=False)
)
df_out['subjectOther_mv'] = df['keywords'].apply(
	lambda keywords: json.dumps([keyword.get('title', '') for keyword in keywords], ensure_ascii=False)
)
df_out['filterType_mv'] = df['types'].apply(
	lambda types: json.dumps([type_entry.get('title', '') for type_entry in types], ensure_ascii=False)
)
df_out['categoryContent_mv'] = df['types'].apply(
	lambda types: json.dumps([type_entry.get('title', '') for type_entry in types], ensure_ascii=False)
)
df_out['categoryMedia_mv'] = df['licenses'].apply(
	lambda licenses: json.dumps(
		[
			license_entry.get('publicationForm', {}).get('title', '')
			for license_entry in licenses
			if license_entry.get('publicationForm')
		],
		ensure_ascii=False,
	)
)
df_out['categoryMedium_mv'] = df['licenses'].apply(
	lambda licenses: json.dumps(
		[
			license_entry.get('publicationForm', {}).get('title', '')
			for license_entry in licenses
			if license_entry.get('publicationForm')
		],
		ensure_ascii=False,
	)
)
df_out['filterMedium_mv'] = df['licenses'].apply(
	lambda licenses: json.dumps(
		[
			license_entry.get('publicationForm', {}).get('title', '')
			for license_entry in licenses
			if license_entry.get('publicationForm')
		],
		ensure_ascii=False,
	)
)
df_out['publisher_display_mv'] = df['licenses'].apply(
	lambda licenses: json.dumps(
		[
			license_entry.get('publisher', {}).get('title', '')
			for license_entry in licenses
			if license_entry.get('publisher')
		],
		ensure_ascii=False,
	)
)
df_out['publisher_id_mv'] = df['licenses'].apply(
	lambda licenses: json.dumps(
		[
			str(license_entry.get('publisher', {}).get('id', ''))
			for license_entry in licenses
			if license_entry.get('publisher')
		],
		ensure_ascii=False,
	)
)
df_out['country_mv'] = df['countries'].apply(
	lambda countries: json.dumps([country.get('title', '') for country in countries], ensure_ascii=False)
)
df_out['url'] = df['licenses'].apply(
	lambda licenses: next(
		(
			access.get('accessUrl', '')
			for license_entry in licenses
			for access in license_entry.get('accesses', [])
			if access.get('accessUrl')
		),
		'',
	)
)
df_out['website_url_mv'] = df['licenses'].apply(
	lambda licenses: json.dumps(
		[
			access.get('accessUrl', '')
			for license_entry in licenses
			for access in license_entry.get('accesses', [])
			if access.get('accessUrl')
		],
		ensure_ascii=False,
	)
)
df_out['website_description_mv'] = df['licenses'].apply(
	lambda licenses: json.dumps(
		[
			access.get('labelLongest')
			or access.get('labelLong')
			or access.get('label')
			or access.get('description')
			or ''
			for license_entry in licenses
			for access in license_entry.get('accesses', [])
		],
		ensure_ascii=False,
	)
)
df_out['digitalObject_display_mv'] = df['licenses'].apply(
	lambda licenses: json.dumps(
		[
			access.get('labelLongest')
			or access.get('labelLong')
			or access.get('label')
			or ''
			for license_entry in licenses
			for access in license_entry.get('accesses', [])
		],
		ensure_ascii=False,
	)
)
df_out['digitalObject_hyperlink_mv'] = df['licenses'].apply(
	lambda licenses: json.dumps(
		[
			access.get('accessUrl', '')
			for license_entry in licenses
			for access in license_entry.get('accesses', [])
			if access.get('accessUrl')
		],
		ensure_ascii=False,
	)
)
df_out['digitalObject_fileExtension_mv'] = df['licenses'].apply(
	lambda licenses: json.dumps(
		sorted(
			{
				access.get('accessUrl', '').split('?', 1)[0].rsplit('.', 1)[-1].lower()
				for license_entry in licenses
				for access in license_entry.get('accesses', [])
				if access.get('accessUrl') and '.' in access.get('accessUrl', '').split('?', 1)[0].rsplit('/', 1)[-1]
			}
		),
		ensure_ascii=False,
	)
)
df_out['digitalObject_license_mv'] = df['licenses'].apply(
	lambda licenses: json.dumps(
		[
			license_entry.get('type', {}).get('title', '')
			for license_entry in licenses
			if license_entry.get('type')
		],
		ensure_ascii=False,
	)
)
df_out['digitalObject_licenseNote_mv'] = df['licenses'].apply(
	lambda licenses: json.dumps(
		[
			license_entry.get('externalNotes', '')
			for license_entry in licenses
			if license_entry.get('externalNotes')
		],
		ensure_ascii=False,
	)
)
df_out['digitalObject_accessLevel_mv'] = df['is_free'].map(
	{True: 'public', False: 'restricted'}
).astype('string')
df_out['titleOther_text_mv'] = df['alternative_titles'].apply(
	lambda alternative_titles: json.dumps(
		[alternative_title.get('title', '') for alternative_title in alternative_titles],
		ensure_ascii=False,
	)
)
df_out['classification_display_mv'] = df['top_resource_entries'].apply(
	lambda entries: json.dumps(
		[
			entry.get('subject', {}).get('title', {}).get('de', '')
			for entry in entries
			if entry.get('subject')
		],
		ensure_ascii=False,
	)
)
df_out['classification_id_mv'] = df['top_resource_entries'].apply(
	lambda entries: json.dumps(
		[
			str(entry.get('subject', {}).get('id', ''))
			for entry in entries
			if entry.get('subject')
		],
		ensure_ascii=False,
	)
)
df_out['vendor_type_mv'] = df['external_ids'].apply(
	lambda external_ids: json.dumps([external_id.get('namespace', '') for external_id in external_ids], ensure_ascii=False)
)
df_out['vendor_id_mv'] = df['external_ids'].apply(
	lambda external_ids: json.dumps([external_id.get('id', '') for external_id in external_ids], ensure_ascii=False)
)
df_out['reference_type_mv'] = df['external_ids'].apply(
	lambda external_ids: json.dumps([external_id.get('id_name', '') for external_id in external_ids], ensure_ascii=False)
)
df_out['reference_text_mv'] = df['external_ids'].apply(
	lambda external_ids: json.dumps(
		[
			f"{external_id.get('namespace', '')}:{external_id.get('id', '')}".strip(':')
			for external_id in external_ids
		],
		ensure_ascii=False,
	)
)
df_out['statusEditing'] = df['traffic_light'].fillna('').astype('string')

df_out = df_out.fillna('')
df_out[df_out.columns] = df_out[df_out.columns].astype('string').replace(
	{r'[\r\n\t]+': ' '}, regex=True
).apply(lambda column: column.str.strip())
df_out.to_csv(args.output, sep='\t', index=False)

print(f'{len(df_out)} Datensaetze transformiert nach {args.output}')
