with raw as (
  select * from read_parquet('c:\git\wikistats\data\enriched\*_enriched.parquet', union_by_name = true)
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
    -- keep as arrays for downstream unnesting
    instance_of, 
    subclass_of, 
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
  from raw left join lang_region using (wiki)
)
select * from cleaned 