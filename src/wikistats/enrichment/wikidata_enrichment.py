import requests
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from urllib.parse import quote
import logging

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
    Fetch labels for multiple Wikidata Q-IDs in batches.
    Returns a dict mapping Q-ID to label (e.g., {'Q5': 'human', 'Q571': 'book'}).
    
    Args:
        qids: List of Q-IDs (e.g., ['Q5', 'Q571'])
        language: Language code for labels (default: 'en')
        batch_size: Number of entities per API request (max 50)
    
    Returns:
        Dict mapping Q-ID -> label
    """
    if not qids:
        return {}
    
    labels = {}
    unique_qids = list(set(qid for qid in qids if qid))  # Remove None and duplicates
    
    # Fetch in batches
    for i in range(0, len(unique_qids), batch_size):
        batch = unique_qids[i:i + batch_size]
        ids_str = "|".join(batch)
        
        params = {
            "action": "wbgetentities",
            "ids": ids_str,
            "format": "json",
            "props": "labels",
            "languages": language
        }
        
        try:
            resp = requests.get(WIKIDATA_API, params=params,
                              headers={"User-Agent": USER_AGENT}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            
            for qid, entity in data.get("entities", {}).items():
                label_data = entity.get("labels", {}).get(language)
                if label_data:
                    labels[qid] = label_data["value"]
                else:
                    # Fallback: use the Q-ID itself if no label found
                    labels[qid] = qid
        except requests.RequestException as e:
            print(f"Warning: Failed to fetch labels for batch {batch}: {e}")
            # Fallback: map Q-IDs to themselves
            for qid in batch:
                labels[qid] = qid
    
    return labels


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


def enrich(files, fetch_remote=True, decode_qids=True):
    """
    Given a set of raw parquet file paths, enrich each with Wikidata information.
    Writes enriched parquet files alongside the raw ones with '_enriched' suffix.
    
    Args:
        files: A set or list of Path objects pointing to raw parquet files
        fetch_remote: Whether to fetch from Wikidata (default: True)
        decode_qids: Whether to decode Wikidata Q-IDs to human-readable labels (default: True)
    """
    print(f"Starting enrichment for {len(files)} files…")

    for file_path in files:
        print(f"\nReading {file_path}…")
        table = pq.read_table(file_path)
        df = table.to_pandas()

        enriched_rows = []
        all_instance_of_qids = []
        all_subclass_of_qids = []

        # First pass: collect all Q-IDs for batch decoding
        for _, row in df.iterrows():
            title = row.get("title")
            wiki = row.get("wiki")

            # Wikidata enrichment
            wd = enrich_article_cached(title, wiki, fetch_remote=fetch_remote)
            
            instance_of_list = wd.get("classification", {}).get("instance_of") or []
            subclass_of_list = wd.get("classification", {}).get("subclass_of") or []
            
            all_instance_of_qids.extend(instance_of_list)
            all_subclass_of_qids.extend(subclass_of_list)

            enriched_rows.append({
                **row.to_dict(),
                "wikidata_id": wd.get("wikidata_id"),
                "instance_of": instance_of_list,
                "subclass_of": subclass_of_list,
            })

        # Batch decode Q-IDs to labels if requested
        if decode_qids and fetch_remote:
            print("Decoding Wikidata Q-IDs in batch...")
            all_qids = list(set(all_instance_of_qids + all_subclass_of_qids))
            qid_labels = get_wikidata_labels_batch(all_qids)
            
            # Replace Q-IDs with labels in enriched_rows
            for row in enriched_rows:
                if row["instance_of"]:
                    row["instance_of"] = [qid_labels.get(qid, qid) for qid in row["instance_of"]]
                if row["subclass_of"]:
                    row["subclass_of"] = [qid_labels.get(qid, qid) for qid in row["subclass_of"]]

        # Convert back to Arrow
        enriched_table = pa.Table.from_pylist(enriched_rows)

        # Write enriched file
        # Move from data/raw/ to data/enriched/
        enriched_dir = file_path.parent.parent / "enriched"
        enriched_dir.mkdir(parents=True, exist_ok=True)
        enriched_path = enriched_dir / file_path.name.replace(".parquet", "_enriched.parquet")

        pq.write_table(enriched_table, enriched_path)

        print(f"Enriched file written → {enriched_path}")

    print("\nEnrichment complete.")
