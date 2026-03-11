# Design Process

## 1. Goal
The proposed dashboard focuses on the two business questions:

1. Top performing products by revenue and volume
2. Optimal time of day for running promotions

The mockup includes charts for:
- product performance
- order distribution by hour
- revenue distribution by hour


## 2. Source Systems
The solution uses:
- PostgreSQL database with customers, orders, and order_items
- PostgreSQL database with product_descriptions
- External currency exchange API
- PostgreSQL as the target data warehouse


## 3. Data Modeling
I chose a star schema because the challenge is focused on analytics and dashboard consumption.

### Dimensions
- dim_customer
- dim_product
- dim_date

### Fact
- fact_sales

This structure keeps the warehouse simple, query-friendly, and aligned with BI tools.


## 4. Transformation Logic
The ETL pipeline:
1. Extracts transactional data from the source PostgreSQL databases
2. Enriches product and customer attributes
3. Calculates gross sales amount at line-item level
4. Converts revenue to USD using exchange rates
5. Loads dimensions first, then the fact table


## 5. Currency Handling
Revenue comparison across countries requires normalization.  
For that reason, I convert local currency amounts into USD.

If an exchange rate is not available, the pipeline keeps the original value and leaves the USD amount null, making the issue explicit instead of silently masking bad data.


## 6. Why line-item grain?
The fact table is modeled at order-item grain because product performance analysis requires product-level aggregation.  
This supports ranking products by:
- units sold
- total revenue


## 7. Dashboard Proposal
I would create a dashboard with:
- Top 10 products by revenue
- Top 10 products by quantity sold
- Orders by hour of day
- Revenue by hour of day
- Optional filters by country, category, and currency


## 8. Trade-offs
Given the time-box of the challenge, I prioritized:
- a clear and scalable warehouse design
- a working ETL flow
- defensive error handling
- straightforward dashboard outputs

Possible next improvements:
- incremental loads
- dbt-style transformations
- data quality checks
- API response caching
- tests and observability
