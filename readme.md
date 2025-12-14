# FilmDB Enhancement Project (CS213 Project 2)

This repository contains the source code, documentation, and tools for **Project 2: Database Enhancement**. The project focuses on updating the `filmdb` database with recent movie data (2019-2025) via the TMDB API and adapting course materials for **openGauss**.

## 🚀 Features

- **Automated ETL Pipeline**: Fetches Top 250 movies annually via TMDB API.
- **Smart Entity Resolution**:
  - Deduplicates against existing database records (`original_data/*.csv`).
  - Reuses existing IDs for actors/directors to maintain referential integrity.
  - Assigns sequential new IDs for new entities.
- **Data Cleaning**: Implements the "Top 4 Cast + 1 Director" strategy and handles data normalization.
- **Cross-Platform Compatibility**: Generates SQL scripts optimized for both **PostgreSQL** and **openGauss**.

## 📂 Project Structure

```text
.
├── data_getter/           # Python scripts for ETL & Logic
│   ├── check_countries.py # Audits and maps country codes
│   ├── people_enricher.py # Fetches detailed birth/death info
│   └── generate_sql_smart.py # Main logic: Data merging & SQL generation
├── original_data/         # CSV exports from existing DB (for deduplication)
│   ├── existing_people.csv
│   └── existing_movies.csv
├── raw_data/              # Raw JSON data fetched from TMDB (ignored)
├── clean_data/            # Processed/Enriched JSON data (ignored)
├── clean_sql/             # Final generated SQL scripts (ignored)
├── report/                # Project Report (LaTeX/PDF)
├── .env                   # Environment variables (API Keys)
└── requirements.txt       # Python dependencies
```

## 🛠️ Setup & Usage

### 1. Prerequisites
- Python 3.8+
- PostgreSQL or openGauss database access

### 2. Installation
Install the required Python libraries:
```bash
pip install -r requirements.txt
```

### 3. Configuration
Create a `.env` file in the root directory and add your TMDB API Key:
```env
TMDB_API_KEY=your_api_key_here
```

### 4. Data Preparation
To enable deduplication, export the current database tables to CSV (no headers) and place them in `original_data/`:
- `original_data/existing_people.csv` (Columns: `id`, `first_name`, `surname`)
- `original_data/existing_movies.csv` (Columns: `id`, `title`, `year`)

### 5. Running the Pipeline

**Step 1: Check Country Codes** (Optional)
Audit mappings between ISO-3166 codes and the local database dialect.
```bash
python data_getter/check_countries.py
```

**Step 2: Generate SQL**
Run the main script. It will fetch/read JSON data, cross-reference with `original_data`, and produce the final SQL.
```bash
python data_getter/generate_sql_smart.py
```

**Step 3: Import to Database**
The generated SQL file will be located in `clean_sql/`. Execute it using your database client:
```bash
# For PostgreSQL/openGauss
psql -d filmdb -f clean_sql/update_filmdb_final.sql
```

## 📝 Report
The detailed project report, including the methodology, openGauss adaptations (MVCC, JSONB, etc.), and lecture notes review, can be found in the `report/` directory.

## 📄 License
This project is for educational purposes (CS213 Course).