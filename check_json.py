import json
data = json.load(open('data/wikidata_labels.json'))
qid = next(iter(data))
print(f'Sample QID: {qid}')
print(f'Sample data: {data[qid]}')
print(f'First seen type: {type(data[qid].get("first_seen_ingestion"))}')
print(f'First seen value repr: {repr(data[qid].get("first_seen_ingestion"))}')
