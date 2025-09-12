from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.udf import udf
import json

# --- UDF ---
@udf(result_type="STRING")
def extract_all_ids(value: str):
    if value is None:
        return None
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list) and len(parsed) > 0:
            return ",".join([str(x) for x in parsed])
    except:
        return value
    return value

#auta einai gia to perivalon
exec_env = StreamExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(4)
env_settings = EnvironmentSettings.in_batch_mode()

t_env = TableEnvironment.create(env_settings)
t_env.get_config().set("parallelism.default", "4")
t_env.get_config().set("python.client.executable", "/home/fneon/flink-2.0.0/bin/venv/bin/python")
t_env.get_config().set("python.fn-execution.bundle.size", "100")  
t_env.get_config().set("python.fn-execution.bundle.time", "1000") 
t_env.get_config().set("taskmanager.memory.task.off-heap.size", "256mb")
t_env.create_temporary_system_function("extract_all_ids", extract_all_ids)


columns_artists = [
    "artist_aliases_name", "artist_aliases_name_id", "artist_groups_name", "artist_groups_name_id",
    "artist_members_name", "artist_members_name_id", "artist_namevariations_name", "artist_urls_url", "artists_artist_data_quality", "artists_artist_id",
    "artists_artist_name", "artists_artist_profile", "artists_artist_realname"
]

columns_releases = [
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
    "releases_release_country", "releases_release_data_quality", "releases_release_id", "releases_release_master_id", "releases_release_master_id_is_main_release", "releases_release_notes", "releases_release_released", "releases_release_status", "releases_release_title",
    "sub_tracks_track_duration", "sub_tracks_track_position", "sub_tracks_track_title",
    "tracklist_track_duration", "tracklist_track_position", "tracklist_track_title",
    "videos_video_description", "videos_video_title"
]
schema_artists = ",\n".join([f"{col_name} STRING" for col_name in columns_artists])
schema_releases = ",\n".join([f"{col_name} STRING" for col_name in columns_releases])


#artists
t_env.execute_sql(f"""
    CREATE TEMPORARY TABLE artists (
        {schema_artists}
    ) WITH (
        'connector' = 'filesystem',
        'path' = '/home/fneon/csv_files/artists.csv',
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true',
        'csv.allow-comments' = 'false',
        'csv.disable-quote-character' = 'false',
        'csv.field-delimiter' = ',',
        'csv.quote-character' = '"'
    )
""")

#releases
t_env.execute_sql(f"""
    CREATE TEMPORARY TABLE releases (
        {schema_releases}
    ) WITH (
        'connector' = 'filesystem',
        'path' = '/home/fneon/csv_files/discogs.csv',
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true',
        'csv.allow-comments' = 'false',
        'csv.disable-quote-character' = 'false',
        'csv.field-delimiter' = ',',
        'csv.quote-character' = '"'
    )
""")

#sampled_discogs
t_env.execute_sql(f"""
    CREATE TEMPORARY TABLE sampled_releases (
        {schema_releases}
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

#sampled_artists
t_env.execute_sql("""
    CREATE TEMPORARY VIEW sampled_artists AS
    SELECT *
    FROM artists a
    WHERE a.artists_artist_id IN (
        SELECT DISTINCT T.artist_id
        FROM sampled_releases r
        CROSS JOIN UNNEST(SPLIT(extract_all_ids(r.artists_artist_id), ',')) AS T(artist_id)
    )
""")


# ------JOIN QUERIES--------

# --- Q1a - Genres per Artist Full Dataset ---
print("\nJoin Query 1 - Top Genres per Artist - Full Dataset") 
res1a = t_env.execute_sql(""" 
SELECT r.release_genres_genre, 
       a.artists_artist_name, 
       COUNT(DISTINCT r.releases_release_id) AS release_count
FROM releases r
CROSS JOIN UNNEST(SPLIT(extract_all_ids(r.artists_artist_id), ',')) AS T(artist_id)
JOIN artists a 
  ON T.artist_id = a.artists_artist_id 
WHERE r.release_genres_genre IS NOT NULL
  AND a.artists_artist_name IS NOT NULL  
GROUP BY r.release_genres_genre, a.artists_artist_name
ORDER BY release_count DESC
LIMIT 20
""") 
res1a.print()

#--- Q1b - Genres per Artists Sample ---

print("\nJoin Query 1 - Top Genres per Artist - Sample") 
res1b = t_env.execute_sql(""" 
SELECT r.release_genres_genre, 
       a.artists_artist_name, 
       COUNT(DISTINCT r.releases_release_id) AS release_count
FROM sampled_releases r
CROSS JOIN UNNEST(SPLIT(extract_all_ids(r.artists_artist_id), ',')) AS T(artist_id)
JOIN sampled_artists a 
  ON T.artist_id = a.artists_artist_id 
WHERE r.release_genres_genre IS NOT NULL
  AND a.artists_artist_name IS NOT NULL  
GROUP BY r.release_genres_genre, a.artists_artist_name
ORDER BY release_count DESC
LIMIT 20
""") 
res1b.print()

# --- Q2a – Artists per Country Full ---

print("\nJoin Query 2 - Artist Names per Country - Full Dataset") 
res2a = t_env.execute_sql(""" 
   SELECT a.artists_artist_name, r.releases_release_country, COUNT(*) AS cnt
   FROM releases r
   CROSS JOIN UNNEST(SPLIT(extract_all_ids(r.artists_artist_id), ',')) AS T(artist_id)
   JOIN artists a 
   ON T.artist_id = a.artists_artist_id 
   WHERE r.releases_release_country IS NOT NULL 
   AND r.releases_release_country <> ''
   AND a.artists_artist_name IS NOT NULL AND a.artists_artist_name <> ''
   GROUP BY a.artists_artist_name, r.releases_release_country
   ORDER BY cnt DESC
   LIMIT 10
""")
res2a.print()

# --- Q2b – Artists per Country Sample ---

print("\nJoin Query 2 - Artist Names per Country - Sample") 
res2b = t_env.execute_sql(""" 
   SELECT a.artists_artist_name, r.releases_release_country, COUNT(*) AS cnt
   FROM sampled_releases r
   CROSS JOIN UNNEST(SPLIT(extract_all_ids(r.artists_artist_id), ',')) AS T(artist_id)
   JOIN sampled_artists a 
   ON T.artist_id = a.artists_artist_id 
   WHERE r.releases_release_country IS NOT NULL 
   AND r.releases_release_country <> ''
   AND a.artists_artist_name IS NOT NULL AND a.artists_artist_name <> ''
   GROUP BY a.artists_artist_name, r.releases_release_country
   ORDER BY cnt DESC
   LIMIT 10
""")
res2b.print()

# --- Q3a - Top 10 artists with the most releases and the number of bands they have participated in, Full---
print("\nJoin Query 3 -  Top 10 artists with the most releases and the number of bands they have participated in - Full") 
res3a = t_env.execute_sql("""
WITH artist_groups AS (
    SELECT a.artists_artist_id,
           a.artists_artist_name,
           a.artist_groups_name_id,
           COUNT(DISTINCT g.group_id) AS cnt_groups
    FROM artists a
    CROSS JOIN UNNEST(SPLIT(extract_all_ids(a.artist_groups_name_id), ',')) AS g(group_id)
    WHERE g.group_id IS NOT NULL AND g.group_id <> ''
    GROUP BY a.artists_artist_id, a.artists_artist_name, a.artist_groups_name_id
),

group_releases AS (
    SELECT g.group_id,
           r.releases_release_id,
           COUNT(DISTINCT r.releases_release_id) AS cnt_releases
    FROM releases r
    CROSS JOIN UNNEST(SPLIT(extract_all_ids(r.artists_artist_id), ',')) AS g(group_id)
    WHERE g.group_id IS NOT NULL AND g.group_id <> ''
    GROUP BY g.group_id,r.releases_release_id
)

SELECT ag.artists_artist_name,
       ag.cnt_groups,
       COUNT(DISTINCT gr.releases_release_id) AS total_releases
FROM artist_groups ag JOIN group_releases gr ON ag.artists_artist_id = gr.group_id
GROUP BY ag.artists_artist_name, ag.cnt_groups
ORDER BY total_releases DESC
LIMIT 10
""")

res3a.print()

# --- Q3b - Top 10 artists with the most releases and the number of bands they have participated in - Sample ---
print("\nJoin Query 3 -  Top 10 artists with the most releases and the number of bands they have participated in - Sample") 
res3b = t_env.execute_sql("""
WITH artist_groups AS (
    SELECT a.artists_artist_id,
           a.artists_artist_name,
           a.artist_groups_name_id,
           COUNT(DISTINCT g.group_id) AS cnt_groups
    FROM sampled_artists a
    CROSS JOIN UNNEST(SPLIT(extract_all_ids(a.artist_groups_name_id), ',')) AS g(group_id)
    WHERE g.group_id IS NOT NULL AND g.group_id <> ''
    GROUP BY a.artists_artist_id, a.artists_artist_name, a.artist_groups_name_id
),

group_releases AS (
    SELECT g.group_id,
           r.releases_release_id,
           COUNT(DISTINCT r.releases_release_id) AS cnt_releases
    FROM sampled_releases r
    CROSS JOIN UNNEST(SPLIT(extract_all_ids(r.artists_artist_id), ',')) AS g(group_id)
    WHERE g.group_id IS NOT NULL AND g.group_id <> ''
    GROUP BY g.group_id,r.releases_release_id
)

SELECT ag.artists_artist_name,
       ag.cnt_groups,
       COUNT(DISTINCT gr.releases_release_id) AS total_releases
FROM artist_groups ag JOIN group_releases gr ON ag.artists_artist_id = gr.group_id
GROUP BY ag.artists_artist_name, ag.cnt_groups
ORDER BY total_releases DESC
LIMIT 10
""")

res3b.print()
