from flask import Flask, request, jsonify
import requests
import json
import unicodedata
import re
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_SEARCH_URL = "https://api.themoviedb.org/3/search/movie"
TMDB_DETAILS_URL = "https://api.themoviedb.org/3/movie/{}"
TMDB_CREDITS_URL = "https://api.themoviedb.org/3/movie/{}/credits"

def normalize_title(title):
    title = unicodedata.normalize("NFKD", title)
    title = "".join(c for c in title if not unicodedata.combining(c))
    title = title.replace("-", " ")
    title = re.sub(r"[^\w\s]", "", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title.lower()

def score_year(tmdb_year, input_year):
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
    except:
        return 0

def score_director(tmdb_credits, input_director):
    if not input_director:
        return 0
    try:
        from thefuzz import fuzz
        directors = [
            member["name"]
            for member in tmdb_credits.get("crew", [])
            if member["job"] == "Director"
        ]
        input_normalized = normalize_title(input_director)
        best_score = 0
        for director in directors:
            director_normalized = normalize_title(director)
            similarity = fuzz.token_sort_ratio(input_normalized, director_normalized)
            best_score = max(best_score, similarity)
        if best_score >= 60:
            return 20
        else:
            return -10
    except:
        return 0

def score_country(tmdb_details, input_country):
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
    except:
        return 0

def get_movie_details(movie_id):
    details_response = requests.get(
        TMDB_DETAILS_URL.format(movie_id),
        params={"api_key": TMDB_API_KEY}
    )
    credits_response = requests.get(
        TMDB_CREDITS_URL.format(movie_id),
        params={"api_key": TMDB_API_KEY}
    )
    return details_response.json(), credits_response.json()

def search_tmdb(query, year=None, director=None, country=None):
    all_movies = []
    existing_ids = set()

    # If year provided, do year-specific searches FIRST so they get priority
    if year:
        for search_year in [int(year), int(year) - 1, int(year) + 1]:
            year_params = {
                "api_key": TMDB_API_KEY,
                "query": query,
                "primary_release_year": search_year
            }
            year_response = requests.get(TMDB_SEARCH_URL, params=year_params)
            year_data = year_response.json()
            for movie in year_data.get("results", []):
                if movie["id"] not in existing_ids:
                    all_movies.append(movie)
                    existing_ids.add(movie["id"])

        # Check for a unique title + year match before doing anything else
        exact_year_matches = []
        for movie in all_movies:
            title_match = (
                movie["title"].lower() == query.lower() or
                normalize_title(movie["title"]) == normalize_title(query)
            )
            tmdb_year = movie.get("release_date", "")[:4]
            try:
                year_match = abs(int(tmdb_year) - int(year)) <= 1
            except:
                year_match = False
            if title_match and year_match:
                exact_year_matches.append(movie)

        # If exactly one film matches title + year, auto-match immediately
        if len(exact_year_matches) == 1:
            movie = exact_year_matches[0]
            return [{
                "id": str(movie["id"]),
                "name": movie["title"],
                "score": 100,
                "match": True,
                "type": [{"id": "movie", "name": "Movie"}]
            }]

    # Add general title search results to fill remaining spots
    general_params = {
        "api_key": TMDB_API_KEY,
        "query": query
    }
    general_response = requests.get(TMDB_SEARCH_URL, params=general_params)
    general_data = general_response.json()
    for movie in general_data.get("results", []):
        if movie["id"] not in existing_ids:
            all_movies.append(movie)
            existing_ids.add(movie["id"])

    # Fall back to full scoring with director, country etc.
    results = []
    for movie in all_movies[:10]:
        exact_match = movie["title"].lower() == query.lower()
        normalized_match = normalize_title(movie["title"]) == normalize_title(query)
        base_score = 60 if (exact_match or normalized_match) else 30

        tmdb_year = movie.get("release_date", "")[:4]
        year_bonus = score_year(tmdb_year, year)

        director_bonus = 0
        country_bonus = 0
        if director or country:
            details, credits = get_movie_details(movie["id"])
            director_bonus = score_director(credits, director)
            country_bonus = score_country(details, country)

        final_score = min(100, base_score + year_bonus + director_bonus + country_bonus)

        results.append({
            "id": str(movie["id"]),
            "name": movie["title"],
            "score": final_score,
            "match": False,
            "type": [{"id": "movie", "name": "Movie"}]
        })

    # Count exact title matches
    exact_matches = [r for r in results if r["score"] >= 60]

    for result in results:
        if len(exact_matches) == 1 and result["score"] >= 60:
            result["match"] = True
        else:
            result["match"] = result["score"] >= 80

    results.sort(key=lambda x: x["score"], reverse=True)
    return results

@app.route("/reconcile", methods=["GET", "POST"])
def reconcile():
    service_metadata = {
        "name": "TMDB Movie Reconciliation",
        "defaultTypes": [{"id": "movie", "name": "Movie"}],
        "view": {"url": "https://www.themoviedb.org/movie/{{id}}"},
        "suggest": {
            "property": {
                "service_url": "http://127.0.0.1:5000",
                "service_path": "/suggest/properties"
            }
        },
        "properties": [
            {"id": "year", "name": "Year"},
            {"id": "director", "name": "Director"},
            {"id": "country", "name": "Country"}
        ]
    }

    queries = request.form.get("queries") or request.args.get("queries")
    callback = request.args.get("callback")

    if not queries:
        response = jsonify(service_metadata)
    else:
        queries = json.loads(queries)
        results = {}
        for key, val in queries.items():
            year = None
            director = None
            country = None
            for prop in val.get("properties", []):
                if prop["pid"] == "year":
                    year = prop["v"]
                elif prop["pid"] == "director":
                    director = prop["v"]
                elif prop["pid"] == "country":
                    country = prop["v"]
            results[key] = {"result": search_tmdb(val["query"], year, director, country)}
        response = jsonify(results)

    if callback:
        return f"{callback}({response.get_data(as_text=True)})", 200, {"Content-Type": "application/javascript"}

    return response

@app.route("/suggest/properties", methods=["GET"])
def suggest_properties():
    properties = [
        {"id": "year", "name": "Year"},
        {"id": "director", "name": "Director"},
        {"id": "country", "name": "Country"}
    ]

    prefix = request.args.get("prefix", "").lower()
    callback = request.args.get("callback")

    filtered = [p for p in properties if prefix in p["name"].lower()]

    result = {"result": filtered}

    if callback:
        return f"{callback}({json.dumps(result)})", 200, {"Content-Type": "application/javascript"}

    return jsonify(result)

if __name__ == "__main__":
    app.run(debug=True, port=5000)