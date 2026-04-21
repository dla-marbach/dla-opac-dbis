#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Transform DBIS-Daten des DLA"""

import argparse
import pandas as pd

__author__ = 'Felix Lohmeier'

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', help='Input file name', default='input/dbis-dla.jsonl')
parser.add_argument('-o', '--output', help='Output file name', default='output/dbis.tsv')
args = parser.parse_args()

df_input = pd.read_json(args.input, lines=True)

df = pd.DataFrame(index=df_input.index)

df['id'] = 'DBIS' + df_input['id'].astype('string')

df['category'] = 'Digitale Nachschlagewerke'
df['categoryMedium_mv'] = 'Online-Ressource'
df['categoryPublication_mv'] = df_input['types'].apply(
	lambda types: ', '.join([type_entry.get('title', '') for type_entry in types if type_entry.get('title')]) or 'Datenbank'
)
df['country_mv'] = df_input['countries'].apply(
	lambda countries: '␟'.join(
		[
			country.get('title', '').strip()
			for country in (countries if isinstance(countries, list) else [])
			if isinstance(country, dict) and country.get('title', '').strip()
		]
	)
)
df['dateCataloged'] = pd.to_datetime(df_input['created_at'], errors='coerce').dt.date.astype('string')
df['dateModified'] = pd.to_datetime(df_input['modified_at'], errors='coerce').dt.date.astype('string')
df['dateOrigin'] = pd.to_datetime(df_input['publication_time_start'], errors='coerce').dt.year.astype('Int64').astype('string').fillna('')
df['dateOriginComment_mv'] = df_input.apply(
	lambda row: (
		f"{str(row.get('publication_time_start')).strip()} - {str(row.get('publication_time_end')).strip()}"
		if pd.notna(row.get('publication_time_start'))
		and str(row.get('publication_time_start')).strip()
		and pd.notna(row.get('publication_time_end'))
		and str(row.get('publication_time_end')).strip()
		else (
			str(row.get('publication_time_start')).strip()
			if pd.notna(row.get('publication_time_start')) and str(row.get('publication_time_start')).strip()
			else (
				str(row.get('publication_time_end')).strip()
				if pd.notna(row.get('publication_time_end')) and str(row.get('publication_time_end')).strip()
				else ''
			)
		)
	),
	axis=1,
)
df['display'] = df_input['title'].fillna('ohne Titel')
df['displayAddition1'] = df_input['types'].apply(
	lambda types: ', '.join([type_entry.get('title', '') for type_entry in types if type_entry.get('title')]) or 'Datenbank'
)
df['displayAddition2'] = df_input.apply(
	lambda row: (
		f"{str(row.get('report_time_start')).strip()} - {str(row.get('report_time_end')).strip()}"
		if pd.notna(row.get('report_time_start'))
		and str(row.get('report_time_start')).strip()
		and pd.notna(row.get('report_time_end'))
		and str(row.get('report_time_end')).strip()
		else (
			str(row.get('report_time_start')).strip()
			if pd.notna(row.get('report_time_start')) and str(row.get('report_time_start')).strip()
			else (
				str(row.get('report_time_end')).strip()
				if pd.notna(row.get('report_time_end')) and str(row.get('report_time_end')).strip()
				else ''
			)
		)
	),
	axis=1,
)
df['displayName'] = df_input.apply(
	lambda row: ', '.join(
		dict.fromkeys(
			[
				author.get('title', '').strip()
				for author in (row.get('authors') if isinstance(row.get('authors'), list) else [])
				if isinstance(author, dict) and author.get('title', '').strip()
			]
			+ [
				license_entry.get('publisher', {}).get('title', '').strip()
				for license_entry in (row.get('licenses') if isinstance(row.get('licenses'), list) else [])
				if isinstance(license_entry, dict)
				and isinstance(license_entry.get('publisher'), dict)
				and license_entry.get('publisher', {}).get('title', '').strip()
			]
		)
	),
	axis=1,
)
df['filterDigital'] = True
df['filterMedium_mv'] = 'Datenbank'
df['filterSource'] = 'Digitale Nachschlagewerke'
df['filterType_mv'] = 'Daten'
df['note'] = df_input.apply(
	lambda row: '. '.join(
		[
			text
			for text in [
				str(row.get('instructions')).strip() if pd.notna(row.get('instructions')) else '',
				str(row.get('note')).strip() if pd.notna(row.get('note')) else '',
			]
			if text
		]
	),
	axis=1,
)
df['noteContent_mv'] = df_input['description']
df['publisherOriginalText_mv'] = df_input.apply(
	lambda row: '␟'.join(
		dict.fromkeys(
			[
				author.get('title', '').strip()
				for author in (row.get('authors') if isinstance(row.get('authors'), list) else [])
				if isinstance(author, dict) and author.get('title', '').strip()
			]
			+ [
				license_entry.get('publisher', {}).get('title', '').strip()
				for license_entry in (row.get('licenses') if isinstance(row.get('licenses'), list) else [])
				if isinstance(license_entry, dict)
				and isinstance(license_entry.get('publisher'), dict)
				and license_entry.get('publisher', {}).get('title', '').strip()
			]
		)
	),
	axis=1,
)
df['source'] = 'AK'
df['subjectOther_mv'] = df_input['keywords'].apply(
	lambda keywords: '␟'.join(
		[
			keyword.get('title', '').strip()
			for keyword in (keywords if isinstance(keywords, list) else [])
			if isinstance(keyword, dict) and keyword.get('title', '').strip()
		]
	)
)
df['textualHolding_mv'] = df_input.apply(
	lambda row: (
		f"{str(row.get('report_time_start')).strip()} - {str(row.get('report_time_end')).strip()}"
		if pd.notna(row.get('report_time_start'))
		and str(row.get('report_time_start')).strip()
		and pd.notna(row.get('report_time_end'))
		and str(row.get('report_time_end')).strip()
		else (
			str(row.get('report_time_start')).strip()
			if pd.notna(row.get('report_time_start')) and str(row.get('report_time_start')).strip()
			else (
				str(row.get('report_time_end')).strip()
				if pd.notna(row.get('report_time_end')) and str(row.get('report_time_end')).strip()
				else ''
			)
		)
	),
	axis=1,
)
df['title'] = df_input['title'].fillna('ohne Titel')
df['titleOther_text_mv'] = df_input['alternative_titles'].apply(
	lambda alternative_titles: '␟'.join(
		[
			alternative_title.get('title', '').strip()
			for alternative_title in (alternative_titles if isinstance(alternative_titles, list) else [])
			if isinstance(alternative_title, dict) and alternative_title.get('title', '').strip()
		]
	)
)
df['titleOther_type_mv'] = df_input['alternative_titles'].apply(
	lambda alternative_titles: '␟'.join(
		[
			'370n'
			for alternative_title in (alternative_titles if isinstance(alternative_titles, list) else [])
			if isinstance(alternative_title, dict) and alternative_title.get('title', '').strip()
		]
	)
)
df['url'] = 'https://www.dla-marbach.de/find/opac/id/' + 'DBIS' + df_input['id'].astype('string')
df['website_url_mv'] = df_input['licenses'].apply(
	lambda licenses: '␟'.join(
		[
			access.get('accessUrl', '').strip()
			for license_entry in (licenses if isinstance(licenses, list) else [])
			if isinstance(license_entry, dict)
			for access in license_entry.get('accesses', [])
			if isinstance(access, dict) and access.get('accessUrl', '').strip()
		]
	)
)
df['website_description_mv'] = df_input['licenses'].apply(
	lambda licenses: '␟'.join(
		[
			(
				(
					access.get('type', {}).get('title', '').strip()
					if isinstance(access.get('type'), dict) and access.get('type', {}).get('title', '').strip()
					else 'Zugang'
				)
				+ (
					f" ({access.get('labelLong').strip()})"
					if isinstance(access.get('labelLong'), str) and access.get('labelLong').strip()
					else ''
				)
			)
			for license_entry in (licenses if isinstance(licenses, list) else [])
			if isinstance(license_entry, dict)
			for access in license_entry.get('accesses', [])
			if isinstance(access, dict) and access.get('accessUrl', '').strip()
		]
	)
)

# Export
df.to_csv(args.output, sep='\t', index=False)
