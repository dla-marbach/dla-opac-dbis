#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Transform DBIS-Daten des DLA"""

import argparse
import json
import pandas as pd

__author__ = 'Felix Lohmeier'

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', help='Input file name', default='input/dbis-dla.jsonl')
parser.add_argument('-o', '--output', help='Output file name', default='output/dbis.tsv')
args = parser.parse_args()

df_input = pd.read_json(args.input, lines=True)

df = pd.DataFrame(index=df_input.index)

df['source'] = 'AK'
df['filterSource'] = 'Digitale Nachschlagewerke'
df['category'] = 'Bibliotheksdokumente'
df['categorySub'] = 'Datenbankressource'
df['id'] = df_input['id'].astype('string')
df['display'] = df_input['title'].fillna('')
df['title'] = df_input['title'].fillna('')
df['titleMain_text'] = df_input['title'].fillna('')
df['displayAddition1'] = df_input['description_short'].fillna('')
df['displayAddition2'] = df_input['types'].apply(
	lambda types: ', '.join([type_entry.get('title', '') for type_entry in types if type_entry.get('title')])
)
df['description_text_mv'] = df_input['description'].fillna('')
df['note'] = df_input['note'].fillna('')
df['usageRestrictionNote'] = df_input['instructions'].fillna('')
df['confidential'] = False
df['filterDigital'] = df_input['licenses'].apply(
	lambda licenses: bool(licenses) and any(
		bool(access.get('accessUrl'))
		for license_entry in licenses
		for access in license_entry.get('accesses', [])
	)
)
df['dateCataloged'] = pd.to_datetime(df_input['created_at'], errors='coerce').dt.date.astype('string')
df['dateModified'] = pd.to_datetime(df_input['modified_at'], errors='coerce').dt.date.astype('string')
df['dateOrigin'] = pd.to_datetime(df_input['publication_time_start'], errors='coerce').dt.year.astype('Int64').astype('string')
df['isbn_mv'] = df_input['isbn_issn'].fillna('').astype('string').str.strip()
df['creator_display_mv'] = df_input['authors'].apply(
	lambda authors: json.dumps([author.get('title', '') for author in authors], ensure_ascii=False)
)
df['creator_id_mv'] = df_input['authors'].apply(
	lambda authors: json.dumps([str(author.get('id', '')) for author in authors], ensure_ascii=False)
)
df['subject_display_mv'] = df_input['subjects'].apply(
	lambda subjects: json.dumps([subject.get('title', '') for subject in subjects], ensure_ascii=False)
)
df['subject_id_mv'] = df_input['subjects'].apply(
	lambda subjects: json.dumps([str(subject.get('id', '')) for subject in subjects], ensure_ascii=False)
)
df['filterSubject_mv'] = df_input['subjects'].apply(
	lambda subjects: json.dumps([subject.get('title', '') for subject in subjects], ensure_ascii=False)
)
df['subjectOther_mv'] = df_input['keywords'].apply(
	lambda keywords: json.dumps([keyword.get('title', '') for keyword in keywords], ensure_ascii=False)
)
df['filterType_mv'] = df_input['types'].apply(
	lambda types: json.dumps([type_entry.get('title', '') for type_entry in types], ensure_ascii=False)
)
df['categoryContent_mv'] = df_input['types'].apply(
	lambda types: json.dumps([type_entry.get('title', '') for type_entry in types], ensure_ascii=False)
)
df['categoryMedia_mv'] = df_input['licenses'].apply(
	lambda licenses: json.dumps(
		[
			license_entry.get('publicationForm', {}).get('title', '')
			for license_entry in licenses
			if license_entry.get('publicationForm')
		],
		ensure_ascii=False,
	)
)
df['categoryMedium_mv'] = df_input['licenses'].apply(
	lambda licenses: json.dumps(
		[
			license_entry.get('publicationForm', {}).get('title', '')
			for license_entry in licenses
			if license_entry.get('publicationForm')
		],
		ensure_ascii=False,
	)
)
df['filterMedium_mv'] = df_input['licenses'].apply(
	lambda licenses: json.dumps(
		[
			license_entry.get('publicationForm', {}).get('title', '')
			for license_entry in licenses
			if license_entry.get('publicationForm')
		],
		ensure_ascii=False,
	)
)
df['publisher_display_mv'] = df_input['licenses'].apply(
	lambda licenses: json.dumps(
		[
			license_entry.get('publisher', {}).get('title', '')
			for license_entry in licenses
			if license_entry.get('publisher')
		],
		ensure_ascii=False,
	)
)
df['publisher_id_mv'] = df_input['licenses'].apply(
	lambda licenses: json.dumps(
		[
			str(license_entry.get('publisher', {}).get('id', ''))
			for license_entry in licenses
			if license_entry.get('publisher')
		],
		ensure_ascii=False,
	)
)
df['country_mv'] = df_input['countries'].apply(
	lambda countries: json.dumps([country.get('title', '') for country in countries], ensure_ascii=False)
)
df['url'] = df_input['licenses'].apply(
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
df['website_url_mv'] = df_input['licenses'].apply(
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
df['website_description_mv'] = df_input['licenses'].apply(
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
df['digitalObject_display_mv'] = df_input['licenses'].apply(
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
df['digitalObject_hyperlink_mv'] = df_input['licenses'].apply(
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
df['digitalObject_fileExtension_mv'] = df_input['licenses'].apply(
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
df['digitalObject_license_mv'] = df_input['licenses'].apply(
	lambda licenses: json.dumps(
		[
			license_entry.get('type', {}).get('title', '')
			for license_entry in licenses
			if license_entry.get('type')
		],
		ensure_ascii=False,
	)
)
df['digitalObject_licenseNote_mv'] = df_input['licenses'].apply(
	lambda licenses: json.dumps(
		[
			license_entry.get('externalNotes', '')
			for license_entry in licenses
			if license_entry.get('externalNotes')
		],
		ensure_ascii=False,
	)
)
df['digitalObject_accessLevel_mv'] = df_input['is_free'].map(
	{True: 'public', False: 'restricted'}
).astype('string')
df['titleOther_text_mv'] = df_input['alternative_titles'].apply(
	lambda alternative_titles: json.dumps(
		[alternative_title.get('title', '') for alternative_title in alternative_titles],
		ensure_ascii=False,
	)
)
df['classification_display_mv'] = df_input['top_resource_entries'].apply(
	lambda entries: json.dumps(
		[
			entry.get('subject', {}).get('title', {}).get('de', '')
			for entry in entries
			if entry.get('subject')
		],
		ensure_ascii=False,
	)
)
df['classification_id_mv'] = df_input['top_resource_entries'].apply(
	lambda entries: json.dumps(
		[
			str(entry.get('subject', {}).get('id', ''))
			for entry in entries
			if entry.get('subject')
		],
		ensure_ascii=False,
	)
)
df['vendor_type_mv'] = df_input['external_ids'].apply(
	lambda external_ids: json.dumps([external_id.get('namespace', '') for external_id in external_ids], ensure_ascii=False)
)
df['vendor_id_mv'] = df_input['external_ids'].apply(
	lambda external_ids: json.dumps([external_id.get('id', '') for external_id in external_ids], ensure_ascii=False)
)
df['reference_type_mv'] = df_input['external_ids'].apply(
	lambda external_ids: json.dumps([external_id.get('id_name', '') for external_id in external_ids], ensure_ascii=False)
)
df['reference_text_mv'] = df_input['external_ids'].apply(
	lambda external_ids: json.dumps(
		[
			f"{external_id.get('namespace', '')}:{external_id.get('id', '')}".strip(':')
			for external_id in external_ids
		],
		ensure_ascii=False,
	)
)
df['statusEditing'] = df_input['traffic_light'].fillna('').astype('string')

df = df.fillna('')
df[df.columns] = df[df.columns].astype('string').replace(
	{r'[\r\n\t]+': ' '}, regex=True
).apply(lambda column: column.str.strip())

# Export
df.to_csv(args.output, sep='\t', index=False)
