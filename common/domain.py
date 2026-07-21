"""
Shared "region" key for this project: the Wikimedia database/project id
(e.g. "enwiki", "dewiki", "commonswiki", "wikidatawiki"). Unlike the
earthquake project's lat/lon grid cell, this key comes straight from
the event -- no bucketing math needed -- but it's still centralised
here so the ingestion, batch, and speed layers all agree on exactly
how to read it and what to do when it's missing.
"""


def wiki_key(event: dict) -> str:
    """Return the wiki/project id for a recentchange event, or 'unknown'
    if the field is missing (defensive -- shouldn't normally happen)."""
    return event.get("wiki") or "unknown"


if __name__ == "__main__":
    print(wiki_key({"wiki": "enwiki"}))
    print(wiki_key({}))
