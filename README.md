# TMDB Reconciliation Service for OpenRefine

A reconciliation service that connects [OpenRefine](https://openrefine.org/) to [The Movie Database (TMDB)](https://www.themoviedb.org/), allowing you to match film titles in your dataset to authoritative TMDB entries.

Built by **Isadora Campregher Paiva**, film historian and lecturer at the University of Amsterdam.

---

## What it does

This service allows you to reconcile a column of film titles in OpenRefine against TMDB, matching them to their unique TMDB IDs. It supports optional additional columns to improve matching accuracy:

* **Year** — matches within a ±2 year margin
* **Director** — fuzzy matching to handle name variations (e.g. "F.W. Murnau", "Friedrich Wilhelm Murnau")
* **Country** — supports co-productions listed with comma separation (e.g. "France, Italy")

The service is designed to handle international cinema well, including films with translated titles, accented characters, and directors whose names are romanised differently across sources.

**Data extension** — after a title is matched with a specific TMDB ID, new columns can be added based on TMDB data, including cast, director, runtime, genres, IMDb ID, budget, revenue, ratings, and more.

---

## Two ways to use it

### Option 1 — Use the public hosted service *(recommended for beginners)*

A hosted version of this service is available at:

```
https://tmdb-reconciliation-openrefine.onrender.com/reconcile
```

**No installation or API key required.** Just add this URL directly to OpenRefine (see [Usage](#usage) below).

> ⚠️ **Note on speed:** The public service runs on a free hosting tier that spins down after periods of inactivity. The first request after an idle period may take 30–50 seconds to respond — OpenRefine may appear to hang briefly. This is normal; it will recover on its own. Subsequent requests will be faster.

---

### Option 2 — Run it locally *(recommended for heavy use or privacy)*

Running the service on your own machine is faster and more reliable for large datasets, and means all requests stay on your computer. It requires a free TMDB API key and a one-time setup.

#### Requirements

* Python 3.x
* A free TMDB API key ([get one here](https://www.themoviedb.org/settings/api))
* OpenRefine ([download here](https://openrefine.org/download))

#### Installation

**1. Clone the repository**

```bash
git clone https://github.com/CamPaiva/tmdb-reconciliation-openrefine.git
cd tmdb-reconciliation-openrefine
```

**2. Install dependencies**

```bash
pip install -r requirements.txt
```

**3. Set up your TMDB API key**

Copy `.env.example`, rename the copy to `.env`, and replace `your_tmdb_api_key_here` with your actual key:

```
TMDB_API_KEY=your_actual_key_here
```

**4. Start the server**

```bash
python reconcile.py
```

You should see:

```
* Running on http://127.0.0.1:5000
```

Then add `http://127.0.0.1:5000/reconcile` to OpenRefine (see [Usage](#usage) below). The server must be running whenever you use the service.

---

## Usage

Once you have either the public URL or a local server running:

**1. Add the service to OpenRefine**

* Open OpenRefine and your project
* Click the dropdown on your film title column → **Reconcile → Start reconciling**
* Click **Add Standard Service** in the bottom left
* Paste in your URL:
  * Public: `https://tmdb-reconciliation-openrefine.onrender.com/reconcile`
  * Local: `http://127.0.0.1:5000/reconcile`

**2. Optionally map additional columns to improve matching**

In the reconciliation dialog, under **"Also use relevant details from other columns"**, you can map:

* A year column → **Year**
* A director column → **Director**
* A country column → **Country**

**Title + year is the recommended combination** — it is fast (no extra API calls) and handles the vast majority of cases well. Adding director or country improves accuracy for ambiguous titles but will slow down reconciliation, as additional API calls are made per candidate.

**3. Add extra columns via data extension**

Once your titles are reconciled, you can pull in additional data from TMDB:

* Click the column dropdown → **Edit column → Add columns from reconciled values**
* Available properties include: genres, director, top cast, release date, runtime, tagline, overview, original title, original language, production countries, production companies, budget, revenue, TMDB rating, vote count, popularity, status, homepage, and IMDb ID

---

## Notes

* Films with common titles (e.g. "Mirror", "Daisies") may require year and/or director information to match correctly
* The public service uses a shared TMDB API key — for very large datasets, running locally is preferable to avoid hitting rate limits

---

## License

This project is licensed under the [MIT License](https://opensource.org/licenses/MIT). See the `LICENSE` file for the full text.

You are free to use, modify, and distribute this software, provided the original copyright notice is retained.

## Citation

If you use this tool in your research, please cite it as:

> Campregher Paiva, Isadora. *TMDB Reconciliation Service for OpenRefine*. University of Amsterdam, 2026. Available at: https://github.com/CamPaiva/tmdb-reconciliation-openrefine
