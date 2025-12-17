# Weather Data Pipeline

Weather data ingestion and transformation for ML.

## Setup

```bash
# 1. Clone repo
git clone <your-repo>
cd weather-pipeline

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure
cp .env.example .env
# Edit .env with your settings
```

## Structure

```
ingestion/     - Python scripts for data ingestion
sql/           - SQL scripts for transformation
notebooks/     - Jupyter notebooks for exploration
```

## Status

- [ ] Setup PostgreSQL
- [ ] Ingest postal codes
- [ ] Ingest weather data
- [ ] Transform data
- [ ] Schedule pipeline

## Next Steps

Start with `notebooks/01_explore_api.ipynb` to explore the data.
