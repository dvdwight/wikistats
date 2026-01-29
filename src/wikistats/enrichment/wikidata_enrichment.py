import requests
from urllib.parse import quote

WIKIDATA_API = "https://www.wikidata.org/w/api.php"

def get_wikidata_id(title, wiki="enwiki"):
    """
    Given a Wikipedia page title, return the corresponding Wikidata Q-ID.
    """
    url = (
        f"https://{wiki.replace('wiki','')}.wikipedia.org/w/api.php"
        f"?action=query&prop=pageprops&titles={quote(title)}&format=json"
    )

    resp = requests.get(url, headers={"User-Agent": "wikistats-enrichment/0.1"})
    resp.raise_for_status()
    data = resp.json()

    pages = data.get("query", {}).get("pages", {})
    for _, page in pages.items():
        props = page.get("pageprops", {})
        if "wikibase_item" in props:
            return props["wikibase_item"]

    return None


def get_wikidata_classification(qid):
    """
    Fetch 'instance of' (P31) and 'subclass of' (P279) for a Wikidata item.
    """
    if not qid:
        return None

    params = {
        "action": "wbgetentities",
        "ids": qid,
        "format": "json",
        "props": "claims"
    }

    resp = requests.get(WIKIDATA_API, params=params, headers={"User-Agent": "wikistats-enrichment/0.1"})
    resp.raise_for_status()
    data = resp.json()

    entity = data.get("entities", {}).get(qid, {})
    claims = entity.get("claims", {})

    def extract_values(prop):
        if prop not in claims:
            return []
        values = []
        for claim in claims[prop]:
            mainsnak = claim.get("mainsnak", {})
            datavalue = mainsnak.get("datavalue", {})
            if datavalue.get("type") == "wikibase-entityid":
                values.append(datavalue["value"]["id"])
        return values

    return {
        "instance_of": extract_values("P31"),
        "subclass_of": extract_values("P279")
    }


def enrich_article(title, wiki="enwiki"):
    """
    Full enrichment: title → Wikidata QID → classification.
    """
    qid = get_wikidata_id(title, wiki)
    classification = get_wikidata_classification(qid)

    return {
        "title": title,
        "wiki": wiki,
        "wikidata_id": qid,
        "classification": classification
    }
