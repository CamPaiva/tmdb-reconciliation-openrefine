# Imports
from flask import Flask, request, jsonify
import requests
import json
import unicodedata
import re
from dotenv import load_dotenv
import os

# Load environment variables from a .env file (e.g. TMDB_API_KEY=your_key)
load_dotenv()

app = Flask(__name__)

# --- TMDB API configuration ---
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

# Base URL for TMDB's movie search endpoint
TMDB_SEARCH_URL = "https://api.themoviedb.org/3/search/movie"

# TMDB movie details endpoint — {id} is replaced at call time.
# We use append_to_response=credits so that details + cast/crew come back
# in a single HTTP request rather than two separate ones.
TMDB_DETAILS_URL = "https://api.themoviedb.org/3/movie/{}"


# ---------------------------------------------------------------------------
# Utility: title normalisation
# ---------------------------------------------------------------------------

def normalize_title(title):
    """
    Normalise a movie title for fuzzy comparison.

    Steps:
      1. NFKD decomposition — splits characters like "é" into base "e" + accent.
      2. Strip combining (accent) characters, so "é" → "e".
      3. Replace hyphens with spaces so "Spider-Man" == "Spider Man".
      4. Remove remaining non-word, non-space characters (punctuation, etc.).
      5. Collapse multiple spaces and lower-case.

    This makes comparisons robust against:
      - Accented characters  (León → leon)
      - Punctuation variants (Se7en, L.A. Confidential)
      - Hyphenation differences
    """
    title = unicodedata.normalize("NFKD", title)
    title = "".join(c for c in title if not unicodedata.combining(c))
    title = title.replace("-", " ")
    title = re.sub(r"[^\w\s]", "", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title.lower()


def titles_match(a, b):
    """
    Return True if two titles are equal after normalisation.
    """
    return normalize_title(a) == normalize_title(b)


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def score_year(tmdb_year, input_year):
    """
    Return a score bonus/penalty based on how closely the TMDB release year
    matches the user-supplied year.

      Exact match  → +20
      Within 2 yrs → +10  (handles films released late in the year vs next year)
      Further off  → -10

    Returns 0 if either year is missing or unparseable.
    """
    if not tmdb_year or not input_year:
        return 0
    try:
        diff = abs(int(tmdb_year) - int(input_year))
        if diff == 0:
            return 20
        elif diff <= 2:
            return 10
        else:
            return -10
    except (ValueError, TypeError):
        return 0


def score_director(tmdb_credits, input_director):
    """
    Return a score bonus/penalty based on whether the user-supplied director
    name appears in the TMDB crew list for this film.

    Uses fuzzy matching (token_sort_ratio) so that:
      - Name order differences ("Kubrick Stanley" vs "Stanley Kubrick") don't matter.
      - Minor transliteration differences still match.

    Threshold: ≥60 similarity → +20 bonus; otherwise −10 penalty.
    Returns 0 if no director was supplied or thefuzz is unavailable.
    """
    if not input_director:
        return 0
    try:
        from thefuzz import fuzz

        # Extract only crew members whose job is "Director"
        directors = [
            member["name"]
            for member in tmdb_credits.get("crew", [])
            if member.get("job") == "Director"
        ]

        input_normalized = normalize_title(input_director)
        best_score = 0
        for director in directors:
            director_normalized = normalize_title(director)
            similarity = fuzz.token_sort_ratio(input_normalized, director_normalized)
            best_score = max(best_score, similarity)

        return 20 if best_score >= 60 else -10
    except Exception:
        # If thefuzz isn't installed or anything else goes wrong, skip quietly
        return 0


def score_country(tmdb_details, input_country):
    """
    Return a score bonus/penalty based on whether any of the user-supplied
    production countries match those recorded in TMDB.

    The input may be a comma-separated list (e.g. "USA, United Kingdom").
    Matching is substring-based so "USA" matches "United States of America"
    and vice-versa.

      Any match  → +10
      No match   → -5
    Returns 0 if no country was supplied.
    """
    if not input_country:
        return 0
    try:
        tmdb_countries = [
            c["name"].lower()
            for c in tmdb_details.get("production_countries", [])
        ]
        input_countries = [c.strip().lower() for c in input_country.split(",")]

        for country in input_countries:
            for tmdb_country in tmdb_countries:
                if country in tmdb_country or tmdb_country in country:
                    return 10
        return -5
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# TMDB API helpers
# ---------------------------------------------------------------------------

def get_movie_details(movie_id):
    """
    Fetch movie details AND credits for a given TMDB movie ID in a single
    HTTP request by using TMDB's append_to_response feature.

    Returns a tuple (details_dict, credits_dict):
      - details_dict: top-level movie metadata (production_countries, etc.)
      - credits_dict: cast/crew data (same as the separate /credits endpoint)

    If the request fails for any reason (network error, non-200 status, bad
    JSON), returns two empty dicts so callers can handle gracefully.
    """
    try:
        response = requests.get(
            TMDB_DETAILS_URL.format(movie_id),
            params={
                "api_key": TMDB_API_KEY,
                # Ask TMDB to bundle the credits response into this single call
                "append_to_response": "credits"
            },
            timeout=10  # Avoid hanging forever if TMDB is slow
        )
        response.raise_for_status()  # Raise an exception for 4xx/5xx responses
        data = response.json()

        # The credits block is nested under the "credits" key in the combined response
        credits = data.pop("credits", {})
        return data, credits

    except requests.exceptions.RequestException as e:
        # Covers connection errors, timeouts, and HTTP error status codes
        print(f"[TMDB] Error fetching details for movie {movie_id}: {e}")
        return {}, {}
    except (ValueError, KeyError) as e:
        # Covers JSON decode errors or unexpected response structure
        print(f"[TMDB] Unexpected response format for movie {movie_id}: {e}")
        return {}, {}


def search_tmdb_api(query, year=None):
    """
    Low-level helper: call the TMDB search endpoint and return a list of
    result dicts. Handles errors gracefully by returning an empty list.

    If a year is provided, it is passed as primary_release_year to TMDB so
    results for that year are ranked higher by TMDB's own algorithm.
    """
    params = {"api_key": TMDB_API_KEY, "query": query}
    if year:
        params["primary_release_year"] = year
    try:
        response = requests.get(TMDB_SEARCH_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"[TMDB] Search request failed for '{query}' (year={year}): {e}")
        return []
    except ValueError as e:
        print(f"[TMDB] Failed to parse search response for '{query}': {e}")
        return []


# ---------------------------------------------------------------------------
# Core reconciliation logic
# ---------------------------------------------------------------------------

def search_tmdb(query, year=None, director=None, country=None):
    """
    Search TMDB for a movie matching the given query and optional filters,
    then score and rank the candidates according to the reconciliation spec.

    Strategy:
      1. If a year is provided, search with year±1 to catch films that straddle
         year boundaries, and collect results deduplicated by TMDB ID.
      2. Check for an "auto-match": exactly one candidate whose normalised title
         AND year both match closely. If found, return it immediately with
         score=100, skipping expensive detail lookups.
      3. Always supplement with a general (no-year) search to catch cases where
         the year is slightly off or TMDB's year-specific ranking missed something.
      4. Score each candidate using title match, year closeness, director
         similarity (fuzzy), and production country.
      5. Set result["match"] = True for results that cross the confidence
         threshold, following the OpenRefine reconciliation spec.

    Returns a list of result dicts sorted by score descending, each with:
      id    - TMDB movie ID (as string)
      name  - Movie title
      score - 0–100 confidence score
      match - True if this result should be auto-accepted by OpenRefine
      type  - Always [{"id": "movie", "name": "Movie"}]
    """
    all_movies = []
    existing_ids = set()  # Track IDs to avoid duplicates across multiple searches

    # ------------------------------------------------------------------
    # Step 1: Year-scoped searches (if year provided)
    # ------------------------------------------------------------------
    if year:
        # Search for year-1, year, and year+1 to handle edge cases like a film
        # shot in 2019 but released in 2020, or inconsistent TMDB metadata.
        for search_year in [int(year) - 1, int(year), int(year) + 1]:
            results = search_tmdb_api(query, year=search_year)
            for movie in results:
                if movie["id"] not in existing_ids:
                    all_movies.append(movie)
                    existing_ids.add(movie["id"])

        # ------------------------------------------------------------------
        # Step 2: Check for an unambiguous auto-match
        # ------------------------------------------------------------------
        # If exactly one candidate matches on both normalised title AND year,
        # we can confidently return it immediately without scoring the rest.
        exact_year_matches = []
        for movie in all_movies:
            tmdb_year_str = movie.get("release_date", "")[:4]
            title_ok = titles_match(movie.get("title", ""), query)
            try:
                year_ok = abs(int(tmdb_year_str) - int(year)) <= 1
            except (ValueError, TypeError):
                year_ok = False

            if title_ok and year_ok:
                exact_year_matches.append(movie)

        if len(exact_year_matches) == 1:
            movie = exact_year_matches[0]
            return [{
                "id": str(movie["id"]),
                "name": movie["title"],
                "score": 100,
                "match": True,
                "type": [{"id": "movie", "name": "Movie"}]
            }]

    # ------------------------------------------------------------------
    # Step 3: General search (no year filter) to fill remaining candidates
    # ------------------------------------------------------------------
    # This catches cases where the year is slightly wrong or missing, and
    # ensures we always have a reasonable pool of candidates to score.
    for movie in search_tmdb_api(query):
        if movie["id"] not in existing_ids:
            all_movies.append(movie)
            existing_ids.add(movie["id"])

    # ------------------------------------------------------------------
    # Step 4: Score each candidate
    # ------------------------------------------------------------------
    results = []
    for movie in all_movies[:10]:  # Cap at 10 to keep response times reasonable
        # Base score: how well does the title match the query?
        #   60 → normalised title is identical (strong match)
        #   30 → title differs (weak match — year/director may pull it up or down)
        if titles_match(movie.get("title", ""), query):
            base_score = 60
        else:
            base_score = 30

        # Year bonus/penalty (see score_year for details)
        tmdb_year = movie.get("release_date", "")[:4]
        year_bonus = score_year(tmdb_year, year)

        # Director and country bonuses — only fetched if the caller supplied
        # these properties, since each fetch is an API call.
        director_bonus = 0
        country_bonus = 0
        if director or country:
            # Single API call returns both details and credits (append_to_response)
            details, credits = get_movie_details(movie["id"])
            director_bonus = score_director(credits, director)
            country_bonus = score_country(details, country)

        # Clamp final score to [0, 100]
        final_score = min(100, max(0, base_score + year_bonus + director_bonus + country_bonus))

        results.append({
            "id": str(movie["id"]),
            "name": movie["title"],
            "score": final_score,
            "match": False,  # Determined below once all results are collected
            "type": [{"id": "movie", "name": "Movie"}]
        })

    # ------------------------------------------------------------------
    # Step 5: Determine which results to auto-match
    # ------------------------------------------------------------------
    # OpenRefine spec: set match=True on a result to auto-accept it.
    # Rules:
    #   - If exactly one result has score ≥ 60 (exact title), auto-match it.
    #     (There's only one plausible film with this title, so it's safe.)
    #   - Otherwise, only auto-match results with score ≥ 80 (very high confidence).
    high_confidence = [r for r in results if r["score"] >= 60]

    for result in results:
        if len(high_confidence) == 1 and result["score"] >= 60:
            # Sole exact-title match — auto-accept even without year confirmation
            result["match"] = True
        else:
            # Multiple candidates or ambiguous title — require a higher bar
            result["match"] = result["score"] >= 80

    # Sort by score descending so OpenRefine shows the best match first
    results.sort(key=lambda x: x["score"], reverse=True)
    return results


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/reconcile", methods=["GET", "POST"])
def reconcile():
    """
    Main reconciliation endpoint, implementing the OpenRefine Reconciliation
    Service API (https://reconciliation-api.github.io/specs/latest/).

    Two modes:
      1. No 'queries' parameter → return service metadata (name, type, etc.)
         OpenRefine calls this when first connecting to the service.
      2. 'queries' parameter present → JSON object of query_key → query_object.
         OpenRefine sends batches of queries; we return results for each.

    Supports JSONP via the 'callback' query parameter for older OpenRefine
    versions that use cross-origin iframe-based communication.

    Optional per-query properties (passed by OpenRefine if the user maps columns):
      year      - Release year (integer or string)
      director  - Director name (string)
      country   - Production country/countries (comma-separated string)
    """
    # Service metadata: tells OpenRefine what this service is and what
    # additional properties it can accept alongside the main title query.
    service_metadata = {
        "name": "TMDB Movie Reconciliation",
        "defaultTypes": [{"id": "movie", "name": "Movie"}],
        # URL template OpenRefine uses to build "view on TMDB" links
        "view": {"url": "https://www.themoviedb.org/movie/{{id}}"},
        # Property suggestion endpoint (lets users search for supported props)
        "suggest": {
            "property": {
                "service_url": "http://127.0.0.1:5000",
                "service_path": "/suggest/properties"
            }
        },
        # Supported additional properties for refining matches
        "properties": [
            {"id": "year",     "name": "Year"},
            {"id": "director", "name": "Director"},
            {"id": "country",  "name": "Country"}
        ]
    }

    # OpenRefine sends queries as either a form field (POST) or query param (GET)
    queries_raw = request.form.get("queries") or request.args.get("queries")
    callback = request.args.get("callback")  # For JSONP support

    if not queries_raw:
        # No queries → return service metadata so OpenRefine can register us
        response = jsonify(service_metadata)
    else:
        # Parse the JSON batch of queries
        queries = json.loads(queries_raw)
        results = {}

        for key, val in queries.items():
            # Extract optional property values if OpenRefine supplied them
            year = director = country = None
            for prop in val.get("properties", []):
                pid = prop.get("pid")
                if pid == "year":
                    year = prop["v"]
                elif pid == "director":
                    director = prop["v"]
                elif pid == "country":
                    country = prop["v"]

            # Run the reconciliation for this query
            results[key] = {
                "result": search_tmdb(val["query"], year, director, country)
            }

        response = jsonify(results)

    # Wrap in JSONP callback if requested (older OpenRefine versions need this)
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
    Property suggestion endpoint.

    OpenRefine calls this when the user types into the "Add property" field
    in the reconciliation dialog, allowing them to discover and select the
    additional properties this service supports (year, director, country).

    Filters properties by the 'prefix' query parameter (case-insensitive).
    Also supports JSONP via 'callback'.
    """
    properties = [
        {"id": "year",     "name": "Year"},
        {"id": "director", "name": "Director"},
        {"id": "country",  "name": "Country"}
    ]

    prefix = request.args.get("prefix", "").lower()
    callback = request.args.get("callback")

    # Return only properties whose name contains the typed prefix
    filtered = [p for p in properties if prefix in p["name"].lower()]
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
    # debug=True enables auto-reload on code changes and detailed error pages.
    # Remove debug=True (or set to False) before deploying to production.
    app.run(debug=True, port=5000)