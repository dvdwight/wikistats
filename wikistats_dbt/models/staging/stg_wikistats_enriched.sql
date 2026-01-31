with raw as (
  select * from read_parquet('../data/enriched/*_enriched.parquet')
),
lang_region AS (
    SELECT * FROM {{ ref('seed_wiki_language_region') }}
),
cleaned as (
  select 
    -- identifiers
    title,
    wiki,
    wikidata_id, 
    -- flatten lists into comma-separated strings 
    array_to_string(instance_of, ',') as instance_of, 
    array_to_string(subclass_of, ',') as subclass_of, 
    region,
    language,
    -- edit metadata 
    user, 
    comment, 
    bot, 
    minor, 
    server_name, 
    length_new, 
    length_old,
    make_timestamp(timestamp * 1000000) as event_timestamp
  from raw join lang_region using (wiki)
)
select * from cleaned 