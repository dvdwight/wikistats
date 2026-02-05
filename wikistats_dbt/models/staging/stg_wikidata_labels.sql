{{ config(materialized='table') }}

with raw as (
  select * from read_json_auto('../data/wikidata_labels.json')
)
select 
  key as qid,
  json_extract_string(value, '$.label') as label,
  json_extract_string(value, '$.description') as description,
  value['instance_of']::json as instance_of,
  value['subclass_of']::json as subclass_of,
  json_extract_string(value, '$.first_seen_ingestion') as first_seen_ingestion,
  json_extract_string(value, '$.last_updated') as last_updated
from raw,
lateral json_each(raw) as t(key, value)