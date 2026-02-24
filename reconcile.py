"""
TMDB Movie Reconciliation & Data Extension Service for OpenRefine
=================================================================
This Flask app implements both the OpenRefine Reconciliation Service API and
the Data Extension API, allowing OpenRefine to:
  1. Match ("reconcile") movie titles against The Movie Database (TMDB).
  2. Extend reconciled rows with additional columns pulled from TMDB — genres,
     runtime, cast, director, tagline, revenue, and more.

Key API flows:
  Registration:  GET  /reconcile           → service metadata (handshake)
  Reconcile:     POST /reconcile?queries=… → scored match candidates
  Data extend:   POST /reconcile?extend=…  → property values for matched IDs
  Prop suggest:  GET  /suggest/properties  → autocomplete in reconcile dialog
  Prop propose:  GET  /propose_properties  → column picker in extend dialog
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

# The base URL OpenRefine will use to build links back to this service.
# Change this if you deploy the service somewhere other than localhost.
SERVICE_BASE_URL = os.getenv("SERVICE_BASE_URL", "http://127.0.0.1:5000")

# ---------------------------------------------------------------------------
# Extension property registry
# ---------------------------------------------------------------------------
# Single source of truth for every property available via data extension.
#
# Fields:
#   id   – Stable identifier used in API requests/responses. Never rename.
#   name – Human-readable label shown in OpenRefine's "Add columns" dialog.
#   type – "str", "int", "float", or "entity".
#           "entity" → value is a named thing (person, genre, etc.) that
#           OpenRefine can render as a linkable, reconcilable cell.
#
# To add a new property:
#   1. Append an entry here.
#   2. Add a matching branch in extract_property_value() below.
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

# Quick lookup by property id
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
    hyphenation (Spider-Man vs Spider Man).
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
    Centralised so all comparisons use identical logic.
    """
    return normalize_title(a) == normalize_title(b)


# ---------------------------------------------------------------------------
# Scoring helpers (reconciliation only)
# ---------------------------------------------------------------------------

def score_year(tmdb_year, input_year):
    """
    Bonus/penalty based on release year proximity.
      Exact       → +20
      Within 2yr  → +10  (catches films that straddle calendar years)
      Further off → -10
    Returns 0 if either value is missing/unparseable.
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
    Bonus/penalty based on fuzzy director name match.
    token_sort_ratio handles name-order differences ("Kubrick Stanley" still
    matches "Stanley Kubrick"). Threshold ≥60 → +20; below → -10.
    Returns 0 if no director supplied or thefuzz unavailable.
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
    Bonus/penalty based on production country match.
    Input may be comma-separated. Substring matching handles abbreviations
    ("USA" ↔ "United States of America").
      Any match → +10;  no match → -5.
    Returns 0 if no country supplied.
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
    Fetch movie details + optional sub-requests in a single HTTP call using
    TMDB's append_to_response feature.

    Parameters:
      movie_id – TMDB numeric movie ID.
      append   – Comma-separated sub-endpoints to bundle (default: "credits").

    Returns (details_dict, credits_dict).
    On any error returns ({}, {}) so callers degrade gracefully.
    """
    try:
        r = requests.get(
            TMDB_DETAILS_URL.format(movie_id),
            params={"api_key": TMDB_API_KEY, "append_to_response": append},
            timeout=10
        )
        r.raise_for_status()
        data    = r.json()
        credits = data.pop("credits", {})
        return data, credits
    except requests.exceptions.RequestException as e:
        print(f"[TMDB] Network error for movie {movie_id}: {e}")
        return {}, {}
    except (ValueError, KeyError) as e:
        print(f"[TMDB] Bad response for movie {movie_id}: {e}")
        return {}, {}


def search_tmdb_api(query, year=None):
    """
    Low-level TMDB movie search wrapper.
    Returns a list of raw result dicts, or [] on any error.
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
    Search TMDB and return a scored, ranked list of candidates for OpenRefine.

    Strategy:
      1. Year-scoped searches (year±1) if year provided.
      2. Auto-match if exactly one candidate matches title + year exactly.
      3. General (no-year) search to fill remaining candidate pool.
      4. Score each candidate on title, year, director, country.
      5. Set match=True based on confidence thresholds.

    Returns list of dicts: {id, name, score, match, type}
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

        # --- Step 2: Auto-match on unambiguous title+year hit ---
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

    # --- Step 3: General search ---
    for movie in search_tmdb_api(query):
        if movie["id"] not in seen_ids:
            all_movies.append(movie)
            seen_ids.add(movie["id"])

    # --- Step 4: Score each candidate ---
    results = []
    for movie in all_movies[:10]:
        base_score    = 60 if titles_match(movie.get("title", ""), query) else 30
        tmdb_year_str = movie.get("release_date", "")[:4]
        year_bonus    = score_year(tmdb_year_str, year)

        director_bonus = country_bonus = 0
        if director or country:
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
    # Sole exact-title match → auto-accept. Multiple candidates → require ≥80.
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
    Extract and format value(s) for one extension property from TMDB data.

    OpenRefine Data Extension cell formats:
      Plain string:  {"str": "some text"}
      Integer:       {"int": 123}
      Float:         {"float": 8.5}
      Named entity:  {"id": "tmdb-id", "name": "Display Name"}

    Entity cells can be further reconciled in OpenRefine against other services.
    Returns a list of cell objects (caller filters out None values).
    Returns [] for unknown property IDs or missing data.
    """

    def str_cell(value):
        return {"str": str(value)} if value else None

    def int_cell(value):
        # TMDB uses 0 for "unknown" budget/revenue — treat as missing
        if value is None: return None
        try:
            v = int(value)
            return {"int": v} if v != 0 else None
        except (ValueError, TypeError):
            return None

    def float_cell(value):
        if value is None: return None
        try:
            return {"float": float(value)}
        except (ValueError, TypeError):
            return None

    def entity_cell(entity_id, name):
        if not name: return None
        return {"id": str(entity_id), "name": str(name)}

    if prop_id == "genres":
        return [entity_cell(g["id"], g["name"]) for g in details.get("genres", []) if g.get("name")]

    elif prop_id == "director":
        return [
            entity_cell(m["id"], m["name"])
            for m in credits.get("crew", [])
            if m.get("job") == "Director" and m.get("name")
        ]

    elif prop_id == "cast":
        # Top 5 billed cast members
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
        cell = str_cell(details.get("original_language"))
        return [cell] if cell else []

    elif prop_id == "original_title":
        cell = str_cell(details.get("original_title"))
        return [cell] if cell else []

    elif prop_id == "production_countries":
        # Use ISO 3166-1 code as stable entity_id
        return [
            entity_cell(c["iso_3166_1"], c["name"])
            for c in details.get("production_countries", [])
            if c.get("name")
        ]

    elif prop_id == "production_companies":
        return [
            entity_cell(c["id"], c["name"])
            for c in details.get("production_companies", [])
            if c.get("name")
        ]

    elif prop_id == "budget":
        cell = int_cell(details.get("budget"))
        return [cell] if cell else []

    elif prop_id == "revenue":
        cell = int_cell(details.get("revenue"))
        return [cell] if cell else []

    elif prop_id == "vote_average":
        cell = float_cell(details.get("vote_average"))
        return [cell] if cell else []

    elif prop_id == "vote_count":
        cell = int_cell(details.get("vote_count"))
        return [cell] if cell else []

    elif prop_id == "popularity":
        cell = float_cell(details.get("popularity"))
        return [cell] if cell else []

    elif prop_id == "status":
        cell = str_cell(details.get("status"))
        return [cell] if cell else []

    elif prop_id == "homepage":
        cell = str_cell(details.get("homepage"))
        return [cell] if cell else []

    elif prop_id == "imdb_id":
        cell = str_cell(details.get("imdb_id"))
        return [cell] if cell else []

    return []  # Unknown property → blank cell


def handle_extend(extend_data):
    """
    Process an OpenRefine Data Extension request.

    Input (from OpenRefine):
      {"ids": ["123", "456"], "properties": [{"id": "genres"}, ...]}

    Output (to OpenRefine):
      {
        "meta": [{"id": "genres", "name": "Genres", "type": {"id": "entity"}}, ...],
        "rows": {
          "123": {"genres": [{"id": "28", "name": "Action"}], ...},
          ...
        }
      }

    "meta" defines column headers + types for the new columns.
    "rows" maps each TMDB movie ID to its property values.
    """
    requested_ids   = extend_data.get("ids", [])
    requested_props = extend_data.get("properties", [])

    # Build meta block
    meta = []
    for prop in requested_props:
        pid  = prop["id"]
        info = PROPERTY_MAP.get(pid, {"id": pid, "name": pid, "type": "str"})
        entry = {"id": pid, "name": info["name"]}
        if info.get("type") == "entity":
            entry["type"] = {"id": "entity"}
        meta.append(entry)

    # Build rows block — one TMDB API call per movie (credits bundled in)
    rows = {}
    for movie_id in requested_ids:
        details, credits = get_movie_details(movie_id, append="credits")
        row = {}
        for prop in requested_props:
            pid      = prop["id"]
            values   = extract_property_value(pid, details, credits)
            row[pid] = [v for v in values if v is not None]
        rows[movie_id] = row

    return {"meta": meta, "rows": rows}


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/reconcile", methods=["GET", "POST"])
def reconcile():
    """
    Combined reconciliation + data extension endpoint.

    Three modes dispatched by which parameter OpenRefine sends:
      (none)    → service metadata JSON (registration handshake)
      queries=… → reconcile movie titles, return scored candidates
      extend=…  → fetch extra columns for already-matched movie IDs

    IMPORTANT — service_metadata fields:
      identifierSpace / schemaSpace:
        Required by OpenRefine 3.1+. Without them OpenRefine has known bugs
        where it silently drops the "extend" block and refuses data extension.
        We use TMDB's own developer URI as the identifierSpace (the canonical
        namespace for TMDB movie IDs) and a local URI for schemaSpace.

      extend.propose_properties:
        Points to our dedicated /propose_properties endpoint (NOT the same as
        /suggest/properties). OpenRefine uses this specifically for the "Add
        columns from reconciled values" dialog.
    """
    service_metadata = {
        "name": "TMDB Movie Reconciliation",
        "defaultTypes": [{"id": "movie", "name": "Movie"}],

        # Required by OpenRefine 3.1+ — absence causes bugs including
        # silently ignoring the "extend" block. Values must be URIs.
        "identifierSpace": "https://www.themoviedb.org/movie/",
        "schemaSpace":     "https://www.themoviedb.org/documentation/api",

        # URL template for "View on TMDB" links in the reconciliation sidebar
        "view": {"url": "https://www.themoviedb.org/movie/{{id}}"},

        # Autocomplete endpoint for the "Add property" field in the
        # reconciliation dialog (used to improve matching with year/director/country)
        "suggest": {
            "property": {
                "service_url":  SERVICE_BASE_URL,
                "service_path": "/suggest/properties"
            }
        },

        # Properties accepted by the reconciliation endpoint to refine matches
        "properties": [
            {"id": "year",     "name": "Year"},
            {"id": "director", "name": "Director"},
            {"id": "country",  "name": "Country"}
        ],

        # Data extension configuration.
        # propose_properties MUST point to a SEPARATE endpoint from suggest/properties.
        # OpenRefine calls propose_properties to populate the column picker in the
        # "Add columns from reconciled values" dialog — it expects the full list of
        # available extension properties, not reconciliation input properties.
        "extend": {
            "propose_properties": {
                "service_url":  SERVICE_BASE_URL,
                "service_path": "/propose_properties"
            },
            "property_settings": []
        }
    }

    queries_raw = request.form.get("queries") or request.args.get("queries")
    extend_raw  = request.form.get("extend")  or request.args.get("extend")
    callback    = request.args.get("callback")

    if queries_raw:
        # Reconciliation mode: score and rank TMDB candidates for each query
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
        # Data extension mode: fetch property values for reconciled movie IDs
        response = jsonify(handle_extend(json.loads(extend_raw)))

    else:
        # Metadata mode: return service descriptor so OpenRefine can register us
        response = jsonify(service_metadata)

    # JSONP support for older OpenRefine versions using cross-origin iframes
    if callback:
        return (
            f"{callback}({response.get_data(as_text=True)})",
            200,
            {"Content-Type": "application/javascript"}
        )

    return response


@app.route("/propose_properties", methods=["GET"])
def propose_properties():
    """
    Data extension property proposal endpoint.

    OpenRefine calls this when the user opens "Add columns from reconciled
    values..." to populate the list of available columns to import.

    This is a SEPARATE endpoint from /suggest/properties (which handles
    autocomplete in the reconciliation input dialog). Mixing the two causes
    OpenRefine to either show no columns or fail to recognise extension support.

    Request params:
      type    – The entity type being extended (we only have "movie"; ignored).
      limit   – Max results to return (optional; we return all matches).
      prefix  – Filter by name prefix (optional).
      callback – JSONP callback (optional).

    Response format (OpenRefine Data Extension spec):
      {
        "type": {"id": "movie", "name": "Movie"},
        "properties": [
          {"id": "genres", "name": "Genres"},
          {"id": "director", "name": "Director"},
          ...
        ]
      }
    """
    prefix   = request.args.get("prefix", "").lower()
    limit    = request.args.get("limit", type=int)
    callback = request.args.get("callback")

    # Filter by prefix if provided
    filtered = [
        p for p in EXTENSION_PROPERTIES
        if prefix in p["name"].lower()
    ]
    if limit:
        filtered = filtered[:limit]

    # Build response in the propose_properties format (note: no "result" wrapper —
    # this endpoint uses a different envelope than suggest/properties)
    result = {
        "type":       {"id": "movie", "name": "Movie"},
        "properties": [{"id": p["id"], "name": p["name"]} for p in filtered]
    }

    if callback:
        return (
            f"{callback}({json.dumps(result)})",
            200,
            {"Content-Type": "application/javascript"}
        )

    return jsonify(result)


@app.route("/suggest/properties", methods=["GET"])
def suggest_properties():
    """
    Property suggestion endpoint for the reconciliation input dialog.

    OpenRefine calls this when the user types in the "Add property" field
    during reconciliation setup, to autocomplete year/director/country.

    This endpoint is ONLY for reconciliation INPUT properties (the ones used
    to improve matching). It is NOT used for data extension column picking —
    that is handled by /propose_properties above.

    Response format (suggest API):
      {"result": [{"id": "year", "name": "Year"}, ...]}
    """
    # Only the reconciliation input properties belong here
    recon_properties = [
        {"id": "year",     "name": "Year"},
        {"id": "director", "name": "Director"},
        {"id": "country",  "name": "Country"}
    ]

    prefix   = request.args.get("prefix", "").lower()
    callback = request.args.get("callback")

    filtered = [p for p in recon_properties if prefix in p["name"].lower()]
    result   = {"result": filtered}

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