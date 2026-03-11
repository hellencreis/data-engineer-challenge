# TypeScript Data Quality Report Generator

This CLI reads a transformed sales CSV file and generates a data quality report in JSON and Markdown.

## Usage

```bash
npm install
npm run dq -- --input ../airflow/data/exports/fact_sales.csv --out ../reports