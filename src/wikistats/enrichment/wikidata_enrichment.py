import requests
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from urllib.parse import quote

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
    except requests.RequestException:
        # Network/DNS errors or non-2xx responses — return None so enrichment can continue
        return None

    pages = data.get("query", {}).get("pages", {})
    for _, page in pages.items():
        props = page.get("pageprops", {})
        if "wikibase_item" in props:
            return props["wikibase_item"]

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


def enrich(files, fetch_remote=True):
    """
    Given a set of raw parquet file paths, enrich each with Wikidata information.
    Writes enriched parquet files alongside the raw ones with '_enriched' suffix.
    
    Args:
        files: A set or list of Path objects pointing to raw parquet files
    """
    print(f"Starting enrichment for {len(files)} files…")

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

            enriched_rows.append({
                **row.to_dict(),
                "wikidata_id": wd.get("wikidata_id"),
                "instance_of": wd.get("classification", {}).get("instance_of"),
                "subclass_of": wd.get("classification", {}).get("subclass_of"),
            })

        # Convert back to Arrow
        enriched_table = pa.Table.from_pylist(enriched_rows)

        # Write enriched file
        enriched_path = Path(str(file_path).replace(".parquet", "_enriched.parquet"))
        
        #susbstitute raw folder with enriched folder
        enriched_path = Path(str(enriched_path).replace("/raw/", "/enriched/"))

        pq.write_table(enriched_table, enriched_path)

        print(f"Enriched file written → {enriched_path}")

    print("\nEnrichment complete.")
