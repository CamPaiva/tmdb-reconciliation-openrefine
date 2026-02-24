"""
TMDB Movie Reconciliation & Data Extension Service for OpenRefine
=================================================================
This Flask app implements both the OpenRefine Reconciliation Service API and
the Data Extension API, allowing OpenRefine to:
  1. Match ("reconcile") movie titles against The Movie Database (TMDB).
  2. Extend reconciled rows with additional columns pulled from TMDB — genres,
     runtime, cast, director, tagline, revenue, and more.

Reconciliation flow:
  OpenRefine → POST /reconcile?queries=... → this service searches TMDB,
  scores candidates, and returns the best matches.

Data extension flow:
  OpenRefine → POST /reconcile?extend=... → this service fetches full TMDB
  details for already-matched movie IDs and returns the requested properties
  as structured values.

Property suggestion flow:
  OpenRefine → GET /suggest/properties?prefix=... → returns the list of
  available extension properties so users can pick them in the UI.
"""

from flask import Flask, request, jsonify
import requests
import json
import unicodedata
import re
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

# ---------------------------------------------------------------------------
# TMDB API configuration
# ---------------------------------------------------------------------------

TMDB_API_KEY     = os.getenv("TMDB_API_KEY")
TMDB_SEARCH_URL  = "https://api.themoviedb.org/3/search/movie"
TMDB_DETAILS_URL = "https://api.themoviedb.org/3/movie/{}"

# ---------------------------------------------------------------------------
# Extension property registry
# ---------------------------------------------------------------------------
# This is the single source of truth for every property available via data
# extension. Each entry has:
#   id      - Stable identifier used in API requests/responses. Never rename.
#   name    - Human-readable label shown in OpenRefine's "Add columns" dialog.
#   type    - "str", "int", "float", or "entity".
#             "entity" means the value is a named thing (person, genre, etc.)
#             and OpenRefine will render it as a linkable reconciled cell.
#
# To add a new property:
#   1. Append an entry to this list.
#   2. Add a branch for its "id" in extract_property_value() below.
# ---------------------------------------------------------------------------

EXTENSION_PROPERTIES = [
    {"id": "genres",               "name": "Genres",                "type": "entity"},
    {"id": "director",             "name": "Director",              "type": "entity"},
    {"id": "cast",                 "name": "Top Cast",              "type": "entity"},
    {"id": "release_date",         "name": "Release Date",          "type": "str"},
    {"id": "runtime",              "name": "Runtime (min)",         "type": "int"},
    {"id": "tagline",              "name": "Tagline",               "type": "str"},
    {"id": "overview",             "name": "Overview",              "type": "str"},
    {"id": "original_language",    "name": "Original Language",     "type": "str"},
    {"id": "original_title",       "name": "Original Title",        "type": "str"},
    {"id": "production_countries", "name": "Production Countries",  "type": "entity"},
    {"id": "production_companies", "name": "Production Companies",  "type": "entity"},
    {"id": "budget",               "name": "Budget (USD)",          "type": "int"},
    {"id": "revenue",              "name": "Revenue (USD)",         "type": "int"},
    {"id": "vote_average",         "name": "TMDB Rating",           "type": "float"},
    {"id": "vote_count",           "name": "Vote Count",            "type": "int"},
    {"id": "popularity",           "name": "Popularity Score",      "type": "float"},
    {"id": "status",               "name": "Status",                "type": "str"},
    {"id": "homepage",             "name": "Homepage",              "type": "str"},
    {"id": "imdb_id",              "name": "IMDb ID",               "type": "str"},
]

# Quick lookup by property id, used in handle_extend() and suggest_properties()
PROPERTY_MAP = {p["id"]: p for p in EXTENSION_PROPERTIES}


# ---------------------------------------------------------------------------
# Utility: title normalisation
# ---------------------------------------------------------------------------

def normalize_title(title):
    """
    Normalise a movie title for fuzzy comparison.

    Steps:
      1. NFKD decomposition — splits "é" into base "e" + combining accent.
      2. Strip combining characters, so "é" → "e".
      3. Replace hyphens with spaces ("Spider-Man" == "Spider Man").
      4. Remove remaining non-word, non-space characters (punctuation).
      5. Collapse multiple spaces and lower-case.

    Handles: accented chars (León→leon), punctuation (L.A. Confidential),
    hyphenation differences (Spider-Man vs Spider Man).
    """
    title = unicodedata.normalize("NFKD", title)
    title = "".join(c for c in title if not unicodedata.combining(c))
    title = title.replace("-", " ")
    title = re.sub(r"[^\w\s]", "", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title.lower()


def titles_match(a, b):
    """
    Return True if two titles are equal after full normalisation.
    Centralising this avoids bugs where some comparisons used only .lower()
    and missed diacritics or punctuation differences.
    """
    return normalize_title(a) == normalize_title(b)


# ---------------------------------------------------------------------------
# Scoring helpers (reconciliation only)
# ---------------------------------------------------------------------------

def score_year(tmdb_year, input_year):
    """
    Score bonus/penalty based on release year proximity.
      Exact match  → +20
      Within 2 yrs → +10  (handles films that straddle calendar years)
      Further off  → -10
    Returns 0 if either year is missing or unparseable.
    """
    if not tmdb_year or not input_year:
        return 0
    try:
        diff = abs(int(tmdb_year) - int(input_year))
        if diff == 0:  return 20
        if diff <= 2:  return 10
        return -10
    except (ValueError, TypeError):
        return 0


def score_director(tmdb_credits, input_director):
    """
    Score bonus/penalty based on fuzzy director name match.

    Uses thefuzz token_sort_ratio so that name order differences
    ("Kubrick Stanley" vs "Stanley Kubrick") don't cause misses.
    Threshold ≥60 similarity → +20; below → -10.
    Returns 0 if no director was supplied or thefuzz is unavailable.
    """
    if not input_director:
        return 0
    try:
        from thefuzz import fuzz
        directors = [
            m["name"] for m in tmdb_credits.get("crew", [])
            if m.get("job") == "Director" and m.get("name")
        ]
        input_norm = normalize_title(input_director)
        best = max(
            (fuzz.token_sort_ratio(input_norm, normalize_title(d)) for d in directors),
            default=0
        )
        return 20 if best >= 60 else -10
    except Exception:
        return 0


def score_country(tmdb_details, input_country):
    """
    Score bonus/penalty based on production country match.

    Input may be comma-separated ("USA, United Kingdom"). Substring matching
    handles common abbreviations ("USA" ↔ "United States of America").
      Any match → +10;  no match → -5.
    Returns 0 if no country was supplied.
    """
    if not input_country:
        return 0
    try:
        tmdb_countries  = [c["name"].lower() for c in tmdb_details.get("production_countries", [])]
        input_countries = [c.strip().lower() for c in input_country.split(",")]
        for ic in input_countries:
            for tc in tmdb_countries:
                if ic in tc or tc in ic:
                    return 10
        return -5
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# TMDB API helpers
# ---------------------------------------------------------------------------

def get_movie_details(movie_id, append="credits"):
    """
    Fetch movie details for a TMDB ID, optionally bundling sub-requests via
    TMDB's append_to_response feature (single HTTP call instead of multiple).

    Parameters:
      movie_id - TMDB numeric movie ID (int or str).
      append   - Comma-separated sub-endpoints to bundle, e.g. "credits".
                 Default is "credits" since director/cast are frequently needed.

    Returns (details_dict, credits_dict):
      details_dict - Top-level metadata: genres, runtime, countries, budget…
      credits_dict - cast and crew lists (empty dict if not appended).

    On any error (network, non-200, bad JSON) returns ({}, {}) so the caller
    can degrade gracefully rather than crashing the whole batch.
    """
    try:
        r = requests.get(
            TMDB_DETAILS_URL.format(movie_id),
            params={"api_key": TMDB_API_KEY, "append_to_response": append},
            timeout=10
        )
        r.raise_for_status()
        data    = r.json()
        credits = data.pop("credits", {})  # Nested when append_to_response is used
        return data, credits
    except requests.exceptions.RequestException as e:
        print(f"[TMDB] Network error fetching movie {movie_id}: {e}")
        return {}, {}
    except (ValueError, KeyError) as e:
        print(f"[TMDB] Bad response for movie {movie_id}: {e}")
        return {}, {}


def search_tmdb_api(query, year=None):
    """
    Low-level wrapper around the TMDB movie search endpoint.
    Returns a list of raw TMDB result dicts, or [] on any error.
    """
    params = {"api_key": TMDB_API_KEY, "query": query}
    if year:
        params["primary_release_year"] = year
    try:
        r = requests.get(TMDB_SEARCH_URL, params=params, timeout=10)
        r.raise_for_status()
        return r.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"[TMDB] Search failed for '{query}' (year={year}): {e}")
        return []
    except ValueError as e:
        print(f"[TMDB] Search parse error for '{query}': {e}")
        return []


# ---------------------------------------------------------------------------
# Core reconciliation logic
# ---------------------------------------------------------------------------

def search_tmdb(query, year=None, director=None, country=None):
    """
    Search TMDB for a movie and return a scored, ranked list of candidates
    formatted for the OpenRefine Reconciliation API.

    Strategy:
      1. If a year is given, search year±1 to handle edge-case release dates.
      2. Auto-match immediately if exactly one candidate matches both title and
         year — skips expensive scoring for unambiguous cases.
      3. Supplement with a general (no-year) search for robustness.
      4. Score each candidate: title match + year/director/country bonuses.
      5. Set match=True based on confidence thresholds.

    Returns a list of dicts, each with:
      id    - TMDB movie ID (string)
      name  - Movie title
      score - 0–100
      match - True if OpenRefine should auto-accept this result
      type  - [{"id": "movie", "name": "Movie"}]
    """
    all_movies = []
    seen_ids   = set()

    # --- Step 1: Year-scoped searches ---
    if year:
        for search_year in [int(year) - 1, int(year), int(year) + 1]:
            for movie in search_tmdb_api(query, year=search_year):
                if movie["id"] not in seen_ids:
                    all_movies.append(movie)
                    seen_ids.add(movie["id"])

        # --- Step 2: Try for an unambiguous auto-match ---
        exact = []
        for movie in all_movies:
            tmdb_yr = movie.get("release_date", "")[:4]
            try:
                year_close = abs(int(tmdb_yr) - int(year)) <= 1
            except (ValueError, TypeError):
                year_close = False
            if titles_match(movie.get("title", ""), query) and year_close:
                exact.append(movie)

        if len(exact) == 1:
            m = exact[0]
            return [{"id": str(m["id"]), "name": m["title"],
                     "score": 100, "match": True,
                     "type": [{"id": "movie", "name": "Movie"}]}]

    # --- Step 3: General search to fill remaining candidates ---
    for movie in search_tmdb_api(query):
        if movie["id"] not in seen_ids:
            all_movies.append(movie)
            seen_ids.add(movie["id"])

    # --- Step 4: Score each candidate (cap at 10 for performance) ---
    results = []
    for movie in all_movies[:10]:
        base_score    = 60 if titles_match(movie.get("title", ""), query) else 30
        tmdb_year_str = movie.get("release_date", "")[:4]
        year_bonus    = score_year(tmdb_year_str, year)

        director_bonus = country_bonus = 0
        if director or country:
            # Single API call returns both details and credits
            details, credits = get_movie_details(movie["id"])
            director_bonus   = score_director(credits, director)
            country_bonus    = score_country(details, country)

        final_score = min(100, max(0, base_score + year_bonus + director_bonus + country_bonus))
        results.append({
            "id":    str(movie["id"]),
            "name":  movie["title"],
            "score": final_score,
            "match": False,
            "type":  [{"id": "movie", "name": "Movie"}]
        })

    # --- Step 5: Set match flags ---
    # If there is only one candidate with an exact title, auto-match it.
    # If there are multiple title matches, require a higher score (≥80) to
    # avoid false positives on common titles (e.g. "The Fly", "Dracula").
    high_conf = [r for r in results if r["score"] >= 60]
    for result in results:
        if len(high_conf) == 1 and result["score"] >= 60:
            result["match"] = True
        else:
            result["match"] = result["score"] >= 80

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


# ---------------------------------------------------------------------------
# Data extension logic
# ---------------------------------------------------------------------------

def extract_property_value(prop_id, details, credits):
    """
    Extract and format the value(s) for one extension property from TMDB data.

    The OpenRefine Data Extension spec expects each property value to be a list
    of "cell objects" in one of these shapes:
      Plain string:  {"str": "some text"}
      Integer:       {"int": 123}
      Float:         {"float": 8.5}
      Named entity:  {"id": "tmdb-id", "name": "Display Name"}

    Entity cells are special: OpenRefine renders them as linkable items that
    can themselves be reconciled against another service later. Use them for
    anything that is a discrete named thing (person, genre, company, country).

    Returns a list of cell objects (may contain None; filter these out in the
    caller). Returns [] for unknown property IDs or missing data.

    To add a new property: add a branch below and a matching entry in
    EXTENSION_PROPERTIES at the top of this file.
    """

    def str_cell(value):
        return {"str": str(value)} if value else None

    def int_cell(value):
        if value is None:
            return None
        try:
            v = int(value)
            # TMDB uses 0 to mean "unknown" for budget/revenue — treat as missing
            return {"int": v} if v != 0 else None
        except (ValueError, TypeError):
            return None

    def float_cell(value):
        if value is None:
            return None
        try:
            return {"float": float(value)}
        except (ValueError, TypeError):
            return None

    def entity_cell(entity_id, name):
        """
        Create an entity cell. entity_id should be a stable identifier
        (TMDB numeric ID, ISO country code, etc.) so OpenRefine can use it
        to reconcile this column against another service if the user wants.
        """
        if not name:
            return None
        return {"id": str(entity_id), "name": str(name)}

    # --- Property branches ---

    if prop_id == "genres":
        # e.g. [{"id": 28, "name": "Action"}, {"id": 12, "name": "Adventure"}]
        return [
            entity_cell(g["id"], g["name"])
            for g in details.get("genres", [])
            if g.get("name")
        ]

    elif prop_id == "director":
        # A film can have multiple directors (co-directed works)
        return [
            entity_cell(m["id"], m["name"])
            for m in credits.get("crew", [])
            if m.get("job") == "Director" and m.get("name")
        ]

    elif prop_id == "cast":
        # Top 5 billed cast members; "order" is TMDB billing order (0 = top)
        top5 = sorted(
            [m for m in credits.get("cast", []) if m.get("name")],
            key=lambda m: m.get("order", 999)
        )[:5]
        return [entity_cell(m["id"], m["name"]) for m in top5]

    elif prop_id == "release_date":
        cell = str_cell(details.get("release_date"))
        return [cell] if cell else []

    elif prop_id == "runtime":
        cell = int_cell(details.get("runtime"))
        return [cell] if cell else []

    elif prop_id == "tagline":
        cell = str_cell(details.get("tagline"))
        return [cell] if cell else []

    elif prop_id == "overview":
        cell = str_cell(details.get("overview"))
        return [cell] if cell else []

    elif prop_id == "original_language":
        # ISO 639-1 code, e.g. "en", "fr", "ja"
        cell = str_cell(details.get("original_language"))
        return [cell] if cell else []

    elif prop_id == "original_title":
        # Title in the film's original language (useful for non-English films)
        cell = str_cell(details.get("original_title"))
        return [cell] if cell else []

    elif prop_id == "production_countries":
        # e.g. [{"iso_3166_1": "US", "name": "United States of America"}]
        # Use ISO code as entity_id so it's stable and reconcilable
        return [
            entity_cell(c["iso_3166_1"], c["name"])
            for c in details.get("production_countries", [])
            if c.get("name")
        ]

    elif prop_id == "production_companies":
        # e.g. [{"id": 420, "name": "Marvel Studios", ...}]
        return [
            entity_cell(c["id"], c["name"])
            for c in details.get("production_companies", [])
            if c.get("name")
        ]

    elif prop_id == "budget":
        # In USD; TMDB stores 0 when unknown (handled by int_cell)
        cell = int_cell(details.get("budget"))
        return [cell] if cell else []

    elif prop_id == "revenue":
        cell = int_cell(details.get("revenue"))
        return [cell] if cell else []

    elif prop_id == "vote_average":
        # TMDB weighted average rating (0.0–10.0)
        cell = float_cell(details.get("vote_average"))
        return [cell] if cell else []

    elif prop_id == "vote_count":
        cell = int_cell(details.get("vote_count"))
        return [cell] if cell else []

    elif prop_id == "popularity":
        # TMDB's internal popularity metric (higher = more popular recently)
        cell = float_cell(details.get("popularity"))
        return [cell] if cell else []

    elif prop_id == "status":
        # e.g. "Released", "Post Production", "Planned", "Canceled"
        cell = str_cell(details.get("status"))
        return [cell] if cell else []

    elif prop_id == "homepage":
        cell = str_cell(details.get("homepage"))
        return [cell] if cell else []

    elif prop_id == "imdb_id":
        # e.g. "tt0133093" — useful for cross-referencing with IMDb or Wikidata
        cell = str_cell(details.get("imdb_id"))
        return [cell] if cell else []

    # Unknown property — return empty list so OpenRefine shows a blank cell
    return []


def handle_extend(extend_data):
    """
    Handle an OpenRefine Data Extension request and return formatted results.

    OpenRefine sends:
      {
        "ids":        ["123", "456"],     ← TMDB movie IDs (already reconciled)
        "properties": [{"id": "genres"}, {"id": "director"}, ...]
      }

    We return:
      {
        "meta": [
          {"id": "genres",   "name": "Genres",   "type": {"id": "entity"}},
          {"id": "director", "name": "Director", "type": {"id": "entity"}},
          ...
        ],
        "rows": {
          "123": {
            "genres":   [{"id": "28", "name": "Action"}, ...],
            "director": [{"id": "578", "name": "James Cameron"}]
          },
          "456": { ... }
        }
      }

    "meta" defines the column headers and types for the new columns.
    "rows" maps each movie ID to its property values for those columns.
    """
    requested_ids   = extend_data.get("ids", [])
    requested_props = extend_data.get("properties", [])

    # Build meta: one entry per requested property.
    # Look up each in our registry; fall back to a generic str entry for
    # unknown IDs so unknown properties don't crash the whole request.
    meta = []
    for prop in requested_props:
        pid  = prop["id"]
        info = PROPERTY_MAP.get(pid, {"id": pid, "name": pid, "type": "str"})
        entry = {"id": pid, "name": info["name"]}
        if info.get("type") == "entity":
            entry["type"] = {"id": "entity"}
        meta.append(entry)

    # Build rows: one entry per movie ID.
    # We always fetch credits alongside details since director and cast are
    # the most commonly requested properties.
    rows = {}
    for movie_id in requested_ids:
        details, credits = get_movie_details(movie_id, append="credits")
        row = {}
        for prop in requested_props:
            pid       = prop["id"]
            values    = extract_property_value(pid, details, credits)
            row[pid]  = [v for v in values if v is not None]
        rows[movie_id] = row

    return {"meta": meta, "rows": rows}


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/reconcile", methods=["GET", "POST"])
def reconcile():
    """
    Combined reconciliation + data extension endpoint.

    OpenRefine uses this single URL for three call types:
      1. No parameters       → return service metadata (registration handshake).
      2. 'queries' parameter → reconcile movie title queries against TMDB.
      3. 'extend' parameter  → fetch extra columns for already-reconciled movies.

    Supports JSONP via the 'callback' query parameter for older OpenRefine
    versions that use cross-origin iframe communication.
    """
    service_metadata = {
        "name": "TMDB Movie Reconciliation",
        "defaultTypes": [{"id": "movie", "name": "Movie"}],
        # URL template for OpenRefine's "View match on TMDB" links
        "view": {"url": "https://www.themoviedb.org/movie/{{id}}"},
        "suggest": {
            "property": {
                "service_url":  "http://127.0.0.1:5000",
                "service_path": "/suggest/properties"
            }
        },
        # Properties that can be used to IMPROVE reconciliation matching
        "properties": [
            {"id": "year",     "name": "Year"},
            {"id": "director", "name": "Director"},
            {"id": "country",  "name": "Country"}
        ],
        # Advertise data extension support to OpenRefine.
        # "propose_properties" tells OpenRefine where to fetch the list of
        # available extension columns (reuses our suggest/properties endpoint).
        "extend": {
            "propose_properties": {
                "service_url":  "http://127.0.0.1:5000",
                "service_path": "/suggest/properties"
            },
            "property_settings": []  # No per-property configuration needed
        }
    }

    queries_raw = request.form.get("queries") or request.args.get("queries")
    extend_raw  = request.form.get("extend")  or request.args.get("extend")
    callback    = request.args.get("callback")

    if queries_raw:
        # --- Mode 2: Reconciliation ---
        queries = json.loads(queries_raw)
        results = {}
        for key, val in queries.items():
            year = director = country = None
            for prop in val.get("properties", []):
                pid = prop.get("pid")
                if pid == "year":       year     = prop["v"]
                elif pid == "director": director = prop["v"]
                elif pid == "country":  country  = prop["v"]
            results[key] = {"result": search_tmdb(val["query"], year, director, country)}
        response = jsonify(results)

    elif extend_raw:
        # --- Mode 3: Data Extension ---
        response = jsonify(handle_extend(json.loads(extend_raw)))

    else:
        # --- Mode 1: Service metadata ---
        response = jsonify(service_metadata)

    if callback:
        return (
            f"{callback}({response.get_data(as_text=True)})",
            200,
            {"Content-Type": "application/javascript"}
        )

    return response


@app.route("/suggest/properties", methods=["GET"])
def suggest_properties():
    """
    Property suggestion endpoint — serves two purposes:

      1. Reconciliation dialog: lets users pick year/director/country to
         improve match quality. (These are listed in service_metadata.properties.)
      2. Data extension dialog: lets users browse and pick which TMDB properties
         to pull in as new columns. (Registered in service_metadata.extend.)

    OpenRefine sends 'prefix' with whatever the user has typed; we return all
    properties whose name contains that substring (case-insensitive).

    Extension properties include a "type" field; OpenRefine uses this to know
    whether to render a column as plain text or as reconcilable entity links.
    """
    prefix   = request.args.get("prefix", "").lower()
    callback = request.args.get("callback")

    filtered = []
    for p in EXTENSION_PROPERTIES:
        if prefix in p["name"].lower():
            entry = {"id": p["id"], "name": p["name"]}
            if p.get("type") == "entity":
                entry["type"] = {"id": "entity"}
            filtered.append(entry)

    result = {"result": filtered}

    if callback:
        return (
            f"{callback}({json.dumps(result)})",
            200,
            {"Content-Type": "application/javascript"}
        )

    return jsonify(result)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Set debug=False before deploying to production.
    app.run(debug=True, port=5000)