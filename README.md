# WikiStats

A data warehouse and analytics platform for Wikipedia relationship data with enrichment from Wikidata.

## Overview

WikiStats ingests, enriches, and visualizes Wikipedia entity relationships. The project combines raw data ingestion with semantic enrichment through Wikidata integration, storing everything in a DuckDB warehouse managed by dbt for transformation and analysis.

## Features

- **Data Ingestion**: Stream and process Wikipedia data
- **Wikidata Enrichment**: Enrich entities with structured data from Wikidata
- **Data Warehouse**: DuckDB-based warehouse with dbt transformations
- **Visualization**: Interactive analytics dashboard via Streamlit
- **Graph Analysis**: Network visualization and analysis tools

## Project Structure

```
wikistats/
├── src/wikistats/              # Main Python package
│   ├── ingestion/              # Data ingestion logic
│   ├── enrichment/             # Wikidata enrichment
│   └── visualization/          # Graph visualization utilities
├── wikistats_dbt/              # dbt project for data transformations
│   ├── models/                 # SQL transformation models
│   ├── seeds/                  # Static data files
│   └── tests/                  # dbt tests
├── notebooks/                  # Jupyter notebooks for exploration
├── data/                       # Raw and processed data
│   ├── raw/                    # Raw input data
│   └── enriched/               # Enriched data outputs
├── warehouse/                  # DuckDB database files
├── streamlit_app.py            # Interactive dashboard
└── pyproject.toml              # Python project configuration
```

## Tech Stack

- **Python** 3.x
- **DuckDB** - Analytical database
- **dbt** - Data transformation and documentation
- **Streamlit** - Interactive web dashboard
- **Wikidata API** - Knowledge base enrichment
- **Jupyter** - Data exploration notebooks

## Getting Started

### Prerequisites

- Python 3.8+
- DuckDB
- dbt

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd wikistats
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
.venv\Scripts\activate  # On Windows
source .venv/bin/activate  # On macOS/Linux
```

3. Install dependencies:
```bash
pip install -e .
```

4. Initialize dbt profiles:
```bash
cd wikistats_dbt
dbt debug
```

## Usage

### Run the Streamlit Dashboard

```bash
streamlit run streamlit_app.py
```

The dashboard will open at `http://localhost:8501`

### Run dbt Transformations

```bash
cd wikistats_dbt
dbt run           # Run all models
dbt test          # Run tests
dbt docs generate # Generate documentation
```

### Data Ingestion

Use the ingestion module to load Wikipedia data:

```python
from src.wikistats.ingestion import stream_ingestion

# Ingest data
stream_ingestion.load_data()
```

### Data Enrichment

Enrich entities with Wikidata information:

```python
from src.wikistats.enrichment import wikidata_enrichment

# Enrich data
wikidata_enrichment.enrich_entities()
```

## Data Flow

1. **Ingestion** → Raw data loaded into DuckDB
2. **Enrichment** → Entities enriched with Wikidata information
3. **Transformation** → dbt models create analytical schemas (staging → mart)
4. **Visualization** → Streamlit dashboard displays insights

## Development

### Exploratory Analysis

Jupyter notebooks are available in the `notebooks/` directory for data exploration:

```bash
jupyter notebook notebooks/explore.ipynb
```

### Running Tests

```bash
cd wikistats_dbt
dbt test
python -m pytest  # For Python unit tests
```

## Project Configuration

Configuration is managed through:
- `pyproject.toml` - Python package and dependency management
- `wikistats_dbt/dbt_project.yml` - dbt project settings
- Environment variables for API keys and credentials
