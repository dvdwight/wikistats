{{ config(materialized='table') }}

with entities as (
  select 
    qid,
    label,
    description,
    instance_of,
    subclass_of,
    first_seen_ingestion,
    last_updated
  from {{ ref('stg_wikidata_labels') }}
),
entities_filtered as (
  select *
  from entities
  where 
    lower(label) not like '%wikimedia%' and 
    lower(label) not like '%wiki %' and
    label !~ '^Q[0-9]+$'
)
select 
  qid,
  label,
  description,
  instance_of,
  subclass_of,
  first_seen_ingestion,
  last_updated
from entities_filtered
