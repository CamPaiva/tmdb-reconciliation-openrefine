# TMDB Reconciliation Service for OpenRefine

A local reconciliation service that connects [OpenRefine](https://openrefine.org/) to [The Movie Database (TMDB)](https://www.themoviedb.org/), allowing you to match film titles in your dataset to authoritative TMDB entries.

Built by **Isadora Campregher Paiva**, film historian and lecturer at the University of Amsterdam.

---

## What it does

This service allows you to reconcile a column of film titles in OpenRefine against TMDB. It supports optional additional columns to improve matching accuracy:

- **Year** — matches within a ±2 year margin
- **Director** — fuzzy matching to handle name variations (e.g. "F.W. Murnau", "Friedrich Wilhelm Murnau")
- **Country** — supports co-productions listed with comma separation (e.g. "France, Italy")

The service is designed to handle international cinema well, including films with translated titles, accented characters, and directors whose names are romanised differently across sources.

Currently, the service reconciles film titles to their corresponding TMDB pages and IDs. Data extension — which will allow users to add new columns from reconciled values (such as cast, director, runtime, genres, and more) — is planned for a future release.

---

## Requirements

- Python 3.x
- A free TMDB API key ([get one here](https://www.themoviedb.org/settings/api))
- OpenRefine ([download here](https://openrefine.org/download))

---

## Installation

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

Copy the example environment file and add your API key:
```bash
cp .env.example .env
```

Then open `.env` and replace `your_tmdb_api_key_here` with your actual key:
```
TMDB_API_KEY=your_actual_key_here
```

---

## Usage

**1. Start the server**
```bash
python reconcile.py
```

You should see:
```
* Running on http://127.0.0.1:5000
```

**2. Add the service to OpenRefine**

- Open OpenRefine and your project
- Click the dropdown on your film title column → **Reconcile → Start reconciling**
- Click **Add Standard Service** in the bottom left
- Enter: `http://127.0.0.1:5000/reconcile`
- Click **Add Service** and select **TMDB Movie Reconciliation**

**3. Optionally map additional columns**

In the reconciliation dialog, under **"Also use relevant details from other columns"**, you can map:

- A year column → **Year**
- A director column → **Director**
- A country column → **Country**

These additional columns significantly improve matching accuracy, especially for international films and films with common titles.

---

## Notes

- The server must be running whenever you use the service in OpenRefine
- **Title + year is the recommended combination** — it is fast (no extra API calls) and handles the vast majority of cases well
- Using director and/or country columns improves accuracy for ambiguous cases but will slow down reconciliation, as additional API calls are made to TMDB for each candidate
- Films with very common translated titles (e.g. "Mirror", "Daisies") may require year and/or director information to match correctly

---

## License

This project is licensed under the [Creative Commons Attribution 4.0 International License (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

You are free to use, share, and adapt this project, but you must give appropriate credit to **Isadora Campregher Paiva** (University of Amsterdam).

If you use this tool in your research, please cite it as:

> Campregher Paiva, Isadora. *TMDB Reconciliation Service for OpenRefine*. University of Amsterdam, 2026. Available at: https://github.com/CamPaiva/tmdb-reconciliation-openrefine
