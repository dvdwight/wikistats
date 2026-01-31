from wikistats.ingestion.stream_ingestion import ingest
from wikistats.enrichment.wikidata_enrichment import enrich

if __name__ == "__main__":
    files = ingest()
    enrich(files)    


