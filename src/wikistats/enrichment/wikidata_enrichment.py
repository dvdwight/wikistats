import requests
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from urllib.parse import quote
import logging
import json
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Simple in‑memory cache to avoid repeated API calls
_CACHE = {}

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
# Descriptive User-Agent required by Wikimedia servers. Replace contact info.
USER_AGENT = "wikistats-enrichment/0.1 (https://github.com/yourname/wikistats; youremail@example.com)"


def get_wikidata_id(title, wiki="enwiki"):
    """
    Given a Wikipedia page title and wiki code (e.g., 'enwiki'),
    return the corresponding Wikidata Q-ID (e.g., 'Q84').
    """
    # Map common `wiki` codes to their API host.
    # Examples:
    #  - enwiki -> en.wikipedia.org
    #  - dewikivoyage -> de.wikivoyage.org
    #  - wikidatawiki -> www.wikidata.org
    prefix = wiki[:-4] if wiki.endswith("wiki") else wiki

    projects = [
        "wikipedia",
        "wikidata",
        "commons",
        "wikivoyage",
        "wiktionary",
        "wikisource",
        "wikibooks",
        "wikiquote",
        "wikinews",
    ]

    host = None
    for proj in projects:
        if prefix.endswith(proj):
            lang = prefix[: -len(proj)]
            if proj == "wikidata":
                host = "www.wikidata.org"
            elif proj == "commons":
                host = f"{lang}.commons.wikimedia.org" if lang else "commons.wikimedia.org"
            elif proj == "wikipedia":
                host = f"{lang}.wikipedia.org" if lang else "www.wikipedia.org"
            else:
                host = f"{lang}.{proj}.org" if lang else f"{proj}.org"
            break

    if not host:
        # Fallback: assume language subdomain on wikipedia
        host = f"{prefix}.wikipedia.org"

    url = (
        f"https://{host}/w/api.php"
        f"?action=query&prop=pageprops&titles={quote(title)}&format=json"
    )

    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        # Network/DNS errors or non-2xx responses — return None so enrichment can continue
        logger.warning(f"Failed to fetch Wikidata ID for '{title}' on {wiki}: {e}")
        return None

    pages = data.get("query", {}).get("pages", {})
    for _, page in pages.items():
        props = page.get("pageprops", {})
        if "wikibase_item" in props:
            return props["wikibase_item"]

    logger.warning(f"No Wikidata ID found for '{title}' on {wiki}")
    return None


def get_wikidata_classification(qid):
    """
    Fetch 'instance of' (P31) and 'subclass of' (P279) for a Wikidata item.
    Returns a dict with lists of Q-IDs.
    """
    if not qid:
        return {"instance_of": None, "subclass_of": None}

    params = {
        "action": "wbgetentities",
        "ids": qid,
        "format": "json",
        "props": "claims"
    }

    resp = requests.get(WIKIDATA_API, params=params,
                        headers={"User-Agent": USER_AGENT}, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    entity = data.get("entities", {}).get(qid, {})
    claims = entity.get("claims", {})

    def extract(prop):
        """Extract Q-IDs from a Wikidata claim property."""
        if prop not in claims:
            return None
        values = []
        for claim in claims[prop]:
            mainsnak = claim.get("mainsnak", {})
            datavalue = mainsnak.get("datavalue", {})
            if datavalue.get("type") == "wikibase-entityid":
                values.append(datavalue["value"]["id"])
        return values or None

    return {
        "instance_of": extract("P31"),
        "subclass_of": extract("P279")
    }


def get_wikidata_labels_batch(qids, language="en", batch_size=50):
    """
    Fetch enriched entity data for multiple Wikidata Q-IDs in batches.
    Returns a dict mapping Q-ID to rich entity object with label, description, and classifications.
    
    Args:
        qids: List of Q-IDs (e.g., ['Q5', 'Q571'])
        language: Language code for labels (default: 'en')
        batch_size: Number of entities per API request (max 50)
    
    Returns:
        Dict mapping Q-ID -> {label, description, instance_of, subclass_of, last_updated}
    """
    if not qids:
        return {}
    
    entities = {}
    unique_qids = list(set(qid for qid in qids if qid))  # Remove None and duplicates
    
    # Fetch in batches
    for i in range(0, len(unique_qids), batch_size):
        batch = unique_qids[i:i + batch_size]
        ids_str = "|".join(batch)
        
        params = {
            "action": "wbgetentities",
            "ids": ids_str,
            "format": "json",
            "props": "labels|descriptions|claims",
            "languages": language
        }
        
        try:
            resp = requests.get(WIKIDATA_API, params=params,
                              headers={"User-Agent": USER_AGENT}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            
            for qid, entity in data.get("entities", {}).items():
                # Extract label
                label_data = entity.get("labels", {}).get(language)
                label = label_data["value"] if label_data else qid
                
                # Extract description
                desc_data = entity.get("descriptions", {}).get(language)
                description = desc_data["value"] if desc_data else None
                
                # Extract classifications
                claims = entity.get("claims", {})
                
                def extract_qids(prop):
                    """Extract Q-IDs from a Wikidata claim property."""
                    if prop not in claims:
                        return None
                    values = []
                    for claim in claims[prop]:
                        mainsnak = claim.get("mainsnak", {})
                        datavalue = mainsnak.get("datavalue", {})
                        if datavalue.get("type") == "wikibase-entityid":
                            values.append(datavalue["value"]["id"])
                    return values if values else None
                
                entities[qid] = {
                    "label": label,
                    "description": description,
                    "instance_of": extract_qids("P31"),
                    "subclass_of": extract_qids("P279"),
                    "last_updated": datetime.now().isoformat()
                }
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch data for batch {batch}: {e}")
            # Fallback: minimal entry
            for qid in batch:
                entities[qid] = {
                    "label": qid,
                    "description": None,
                    "instance_of": None,
                    "subclass_of": None,
                    "last_updated": datetime.now().isoformat()
                }
    
    return entities


def enrich_article(title, wiki="enwiki", fetch_remote=True):
    """
    Full enrichment pipeline for a single article:
      - title → Wikidata Q-ID
      - Q-ID → classification (instance_of, subclass_of)
    """
    if not fetch_remote:
        return {
            "title": title,
            "wiki": wiki,
            "wikidata_id": None,
            "classification": {"instance_of": None, "subclass_of": None},
        }

    qid = get_wikidata_id(title, wiki)
    classification = get_wikidata_classification(qid)

    return {
        "title": title,
        "wiki": wiki,
        "wikidata_id": qid,
        "classification": classification
    }


def enrich_article_cached(title, wiki="enwiki", fetch_remote=True):
    """
    Cached version of enrich_article() to avoid repeated API calls.
    Perfect for enrichment of many rows where titles repeat.
    """
    key = (title, wiki)
    if key in _CACHE:
        return _CACHE[key]

    try:
        result = enrich_article(title, wiki, fetch_remote=fetch_remote)
    except requests.RequestException:
        # Network-related errors — return a safe placeholder so enrichment can continue
        result = {
            "title": title,
            "wiki": wiki,
            "wikidata_id": None,
            "classification": {"instance_of": None, "subclass_of": None},
        }
    except Exception:
        # Any other unexpected error — don't crash the whole pipeline
        result = {
            "title": title,
            "wiki": wiki,
            "wikidata_id": None,
            "classification": {"instance_of": None, "subclass_of": None},
        }

    _CACHE[key] = result
    return result


def merge_entity_data(existing_path, new_entities, ingestion_timestamp):
    """
    Merge new entity data with existing labels.json, preserving first_seen_ingestion dates.
    
    Args:
        existing_path: Path to existing wikidata_labels.json
        new_entities: Dict of new entity data from get_wikidata_labels_batch()
        ingestion_timestamp: ISO timestamp of current ingestion
    
    Returns:
        Merged entity dict
    """
    merged = {}
    
    # Load existing if present
    if existing_path.exists():
        with open(existing_path, 'r') as f:
            merged = json.load(f)
    
    # Merge new entities
    for qid, entity_data in new_entities.items():
        if qid in merged:
            # Update existing entity, preserve first_seen_ingestion
            merged[qid].update(entity_data)
        else:
            # New entity, set first_seen_ingestion
            entity_data["first_seen_ingestion"] = ingestion_timestamp
            merged[qid] = entity_data
    
    return merged


def generate_label_mappings(fetch_remote=True, base_dir=None):
    """
    Generate rich Q-ID entity data from all existing enriched parquet files.
    Useful for regenerating the wikidata_labels.json after bulk enrichment.
    
    Args:
        fetch_remote: Whether to fetch from Wikidata (default: True)
        base_dir: Base directory of the project (default: automatically detected)
    """
    if base_dir is None:
        # Try to use __file__ if available (when imported as module)
        try:
            base_dir = Path(__file__).resolve().parents[3]
        except:
            # Fallback: use current working directory
            base_dir = Path.cwd()
    else:
        base_dir = Path(base_dir)
    
    enriched_dir = base_dir / "data" / "enriched"
    
    if not enriched_dir.exists():
        print(f"No enriched directory found at {enriched_dir}")
        return
    
    enriched_files = list(enriched_dir.glob("*_enriched.parquet"))
    
    if not enriched_files:
        print(f"No enriched parquet files found in {enriched_dir}")
        return
    
    print(f"Collecting Q-IDs from {len(enriched_files)} enriched files…")
    
    all_qids = set()
    
    for file_path in enriched_files:
        print(f"Reading {file_path.name}…")
        table = pq.read_table(file_path)
        df = table.to_pandas()
        
        for _, row in df.iterrows():
            instance_of_list = row.get("instance_of") if row.get("instance_of") is not None else []
            subclass_of_list = row.get("subclass_of") if row.get("subclass_of") is not None else []
            
            # Handle both lists and lists stored as strings
            if isinstance(instance_of_list, str):
                instance_of_list = instance_of_list.strip("[]").split(",") if instance_of_list else []
            if isinstance(subclass_of_list, str):
                subclass_of_list = subclass_of_list.strip("[]").split(",") if subclass_of_list else []
            
            all_qids.update(instance_of_list)
            all_qids.update(subclass_of_list)
    
    # Fetch enriched data for all unique Q-IDs
    if fetch_remote:
        print("\nFetching Wikidata entity data for all Q-IDs…")
        all_qids = list(all_qids)
        new_entities = get_wikidata_labels_batch(all_qids)
        
        # Merge with existing
        labels_dir = base_dir / "data"
        labels_dir.mkdir(parents=True, exist_ok=True)
        labels_path = labels_dir / "wikidata_labels.json"
        
        ingestion_timestamp = datetime.now().isoformat()
        merged = merge_entity_data(labels_path, new_entities, ingestion_timestamp)
        
        with open(labels_path, 'w') as f:
            json.dump(merged, f, indent=2)
        
        print(f"Q-ID entity data written → {labels_path}")


def enrich(files, fetch_remote=True):
    """
    Given a set of raw parquet file paths, enrich each with Wikidata information.
    Writes enriched parquet files with Q-IDs intact.
    Also updates wikidata_labels.json with rich entity data (label, description, classifications).
    
    Args:
        files: A set or list of Path objects pointing to raw parquet files
        fetch_remote: Whether to fetch from Wikidata (default: True)
    """
    print(f"Starting enrichment for {len(files)} files…")

    all_qids = set()
    ingestion_timestamp = datetime.now().isoformat()

    for file_path in files:
        print(f"\nReading {file_path}…")
        table = pq.read_table(file_path)
        df = table.to_pandas()

        enriched_rows = []

        for _, row in df.iterrows():
            title = row.get("title")
            wiki = row.get("wiki")

            # Wikidata enrichment
            wd = enrich_article_cached(title, wiki, fetch_remote=fetch_remote)
            
            instance_of_list = wd.get("classification", {}).get("instance_of") or []
            subclass_of_list = wd.get("classification", {}).get("subclass_of") or []
            
            all_qids.update(instance_of_list)
            all_qids.update(subclass_of_list)

            enriched_rows.append({
                **row.to_dict(),
                "wikidata_id": wd.get("wikidata_id"),
                "instance_of": instance_of_list,
                "subclass_of": subclass_of_list,
            })

        # Convert back to Arrow
        enriched_table = pa.Table.from_pylist(enriched_rows)

        # Write enriched file
        # Move from data/raw/ to data/enriched/
        enriched_dir = file_path.parent.parent / "enriched"
        enriched_dir.mkdir(parents=True, exist_ok=True)
        enriched_path = enriched_dir / file_path.name.replace(".parquet", "_enriched.parquet")

        pq.write_table(enriched_table, enriched_path)

        print(f"Enriched file written → {enriched_path}")

    # After all files processed, fetch enriched entity data for all unique Q-IDs
    if fetch_remote and all_qids:
        print("\nFetching Wikidata entity data for all Q-IDs...")
        new_entities = get_wikidata_labels_batch(list(all_qids))
        
        # Merge with existing labels
        labels_dir = Path(__file__).resolve().parents[3] / "data"
        labels_dir.mkdir(parents=True, exist_ok=True)
        labels_path = labels_dir / "wikidata_labels.json"
        
        merged = merge_entity_data(labels_path, new_entities, ingestion_timestamp)
        
        with open(labels_path, 'w') as f:
            json.dump(merged, f, indent=2)
        
        print(f"Q-ID entity data written → {labels_path}")

    print("\nEnrichment complete.")
