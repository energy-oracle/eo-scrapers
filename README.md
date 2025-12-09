# eo-scrapers

Data ingestion layer for EnergyOracle - scraping European energy market data.

## Data Sources

- **Italy:** GME (PUN), Terna (grid), GSE (REC), ARERA (tariffs)
- **Germany:** EPEX, Bundesnetzagentur (planned)
- **France:** RTE, EPEX (planned)

## Tech Stack

- Python 3.11+
- Scrapy / httpx / BeautifulSoup4
- Celery + Redis
- Apache Airflow

## Setup

```bash
# Install dependencies
pip install -e ".[dev]"

# Run scraper
python -m scrapers.italy.gme_pun
```

## License

MIT
