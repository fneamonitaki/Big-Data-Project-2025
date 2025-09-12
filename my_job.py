from pyflink.table import (
    EnvironmentSettings, TableEnvironment
)
from pyflink.table.udf import udf
from pyflink.datastream import StreamExecutionEnvironment
import json


# --- UDFs ---
@udf(result_type="BOOLEAN")
def has_style(value, target):
    if value is None:
        return False
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return target in parsed
    except:
        pass
    if isinstance(value, str):
        return target.lower() in value.lower()
    return False

@udf(result_type="INT")
def track_count(value):
    if value is None:
        return 0
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return len(parsed)
    except:
        return 0
    return 0

@udf(result_type="BOOLEAN")
def has_artist(value, target):
    if value is None:
        return False
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return target in parsed
    except:
        pass
    if isinstance(value, str):
        return target.lower() in value.lower()
    return False

@udf(result_type="BOOLEAN")
def has_country(value, target):
    if value is None:
        return False
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return target in parsed
    except:
        pass
    if isinstance(value, str):
        return target.lower() in value.lower()
    return False


#enviroment settings
exec_env = StreamExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(4)

env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

t_env.get_config().set("parallelism.default", "4")

t_env.create_temporary_system_function("has_style", has_style)
t_env.create_temporary_system_function("has_artist", has_artist)
t_env.create_temporary_system_function("has_country", has_country)
t_env.create_temporary_system_function("track_count", track_count)

columns = [
    "artists_artist_anv", "artists_artist_id", "artists_artist_join", "artists_artist_name",
    "companies_company_catno", "companies_company_entity_type", "companies_company_entity_type_name", "companies_company_id", "companies_company_name", "companies_company_resource_url",
    "extraartists_artist_anv", "extraartists_artist_id", "extraartists_artist_name", "extraartists_artist_role", "extraartists_artist_tracks",
    "format_descriptions_description", "release_formats_format_name", "release_formats_format_qty", "release_formats_format_text",
    "release_genres_genre",
    "release_identifiers_identifier_description", "release_identifiers_identifier_type", "release_identifiers_identifier_value",
    "release_labels_label_catno", "release_labels_label_id", "release_labels_label_name",
    "release_series_series_catno", "release_series_series_id", "release_series_series_name",
    "release_styles_style",
    "release_videos_video_duration", "release_videos_video_embed", "release_videos_video_src",
    "releases_release_country", "releases_release_data_quality", "releases_release_id", "releases_release_master_id", 
    "releases_release_master_id_is_main_release", "releases_release_notes", "releases_release_released", 
    "releases_release_status", "releases_release_title",
    "sub_tracks_track_duration", "sub_tracks_track_position", "sub_tracks_track_title",
    "tracklist_track_duration", "tracklist_track_position", "tracklist_track_title",
    "videos_video_description", "videos_video_title"
]

schema = ",\n".join([f"{col_name} STRING" for col_name in columns])

t_env.execute_sql(f"""
    CREATE TEMPORARY TABLE my_table (
        {schema}
    ) WITH (
        'connector' = 'filesystem',
        'path' = '/home/fneon/csv_files/discogs_sample_1.csv',
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true',
        'csv.allow-comments' = 'false',
        'csv.disable-quote-character' = 'false',
        'csv.field-delimiter' = ',',
        'csv.quote-character' = '"'
    )
""")

#--- Queries ---

print("\n[1] Top 10 Formats")
res1 = t_env.execute_sql("""
    SELECT release_formats_format_name,
        COUNT(*) AS cnt
    FROM my_table
    WHERE release_formats_format_name IS NOT NULL
    AND release_formats_format_name <> ''
    GROUP BY release_formats_format_name
    ORDER BY cnt DESC
    LIMIT 10
""")
res1.print()

print("\n[2] Top 10 Styles")
res2 = t_env.execute_sql("""
    SELECT release_styles_style,
        COUNT(*) AS cnt
    FROM my_table
    WHERE release_styles_style IS NOT NULL
    AND release_styles_style <> ''
    GROUP BY release_styles_style
    ORDER BY cnt DESC
    LIMIT 10
""")
res2.print()

print("\n[3] Top 20 Artists with most releases")
res3 = t_env.execute_sql("""
    SELECT artists_artist_name,
        COUNT(DISTINCT releases_release_id) AS cnt
    FROM my_table
    WHERE artists_artist_name IS NOT NULL
    AND artists_artist_name <> ''
    GROUP BY artists_artist_name
    ORDER BY cnt DESC
    LIMIT 20
""")
res3.print()

print("\n[4] Top 10 Metal Artists")
res4 = t_env.execute_sql("""
    SELECT artists_artist_name,
           COUNT(*) AS release_count
    FROM my_table
    WHERE has_style(release_styles_style, 'Metal')
          AND artists_artist_name IS NOT NULL
    GROUP BY artists_artist_name
    ORDER BY release_count DESC
    LIMIT 10
""")
res4.print()


print("\n[5] Artists with most genres")
res5 = t_env.execute_sql("""
    SELECT artists_artist_name,
        COUNT(DISTINCT release_genres_genre) AS genre_count
    FROM my_table
    WHERE release_genres_genre IS NOT NULL
    AND release_genres_genre <> ''
    AND artists_artist_name IS NOT NULL
    AND artists_artist_name <> ''
    GROUP BY artists_artist_name
    ORDER BY genre_count DESC
    LIMIT 10
""")
res5.print()

print("\n[6] Top 10 Artists in Greece with most releases")
res6 = t_env.execute_sql("""
    SELECT artists_artist_name,
        COUNT(*) AS cnt
    FROM my_table
    WHERE releases_release_country IS NOT NULL
    AND has_country(releases_release_country, 'Greece')
    AND artists_artist_name IS NOT NULL
    AND artists_artist_name <> ''
    GROUP BY artists_artist_name
    ORDER BY cnt DESC
    LIMIT 10
""")
res6.print()

print("\n[7] Average number of tracks per release")
res7 = t_env.execute_sql("""
    SELECT AVG(track_count(tracklist_track_duration)) AS avg_tracks
    FROM my_table
    
""")
res7.print()

res8 = t_env.execute_sql("""
WITH year_counts AS (
    SELECT
        SUBSTRING(releases_release_released, 1, 4) AS year_part,
        COUNT(DISTINCT releases_release_id) AS release_count
    FROM my_table
    WHERE releases_release_released IS NOT NULL
      AND CHAR_LENGTH(SUBSTRING(releases_release_released, 1, 4)) = 4
    GROUP BY SUBSTRING(releases_release_released, 1, 4)
)
SELECT
    AVG(release_count) AS avg_releases_per_year,
    MAX(CASE WHEN year_part = '1999' THEN release_count END) AS releases_in_1999
FROM year_counts
""")
res8.print()


