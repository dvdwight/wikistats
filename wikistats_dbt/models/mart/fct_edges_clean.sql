{{ config(materialized='table') }}

with edges as (
  select 
    source_qid,
    target_qid,
    relationship_type,
    source_label,
    target_label
  from {{ ref('dim_edges') }}
),
clean_entities as (
  select qid from {{ ref('dim_entities_clean') }}
),
edges_filtered as (
  select *
  from edges
  where 
    -- Both source and target must be in the clean entities list
    source_qid in (select qid from clean_entities) and
    target_qid in (select qid from clean_entities)
)
select 
  source_qid,
  target_qid,
  relationship_type,
  source_label,
  target_label
from edges_filtered
