{{ config(materialized='table') }}

with enriched as (
  select * from {{ ref('stg_wikistats_enriched') }}
),
labels as (
  select * from {{ ref('stg_wikidata_labels') }}
),
instance_edges as (
  select 
    wikidata_id as source_qid,
    unnest(instance_of) as target_qid,
    'instance_of' as relationship_type
  from enriched
  where instance_of is not null
),
subclass_edges as (
  select 
    wikidata_id as source_qid,
    unnest(subclass_of) as target_qid,
    'subclass_of' as relationship_type
  from enriched
  where subclass_of is not null
),
edges as (
  select * from instance_edges
  union all
  select * from subclass_edges
),
edges_with_labels as (
  select 
    e.source_qid,
    e.target_qid,
    e.relationship_type,
    l_source.label as source_label,
    l_target.label as target_label
  from edges e
  left join labels l_source on e.source_qid = l_source.qid
  left join labels l_target on e.target_qid = l_target.qid
)
select * from edges_with_labels