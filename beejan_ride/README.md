# Production-Grade Analytics Platform with dbt
BeejanRide, a UK mobility startup operating in five cities with prospect of scaling within the next couple of years. This startup is keen to take advantage of modern data stack tool dbt and requested the services of a data engineer to implement dbt to enable production grade analytics platform to power analytics, business operations, finance and timely fraud detection.

 BeejanRide provides the following services:
   * Ride-hailing
   * Airport transfers
   * Scheduled corporate rides
Dbt provides a simplified environment for SQL/python tranformation logic in a modular approach that enhances business operations, logics and governance. It features built-in testing and data quality with native test (not_null, unique, accepted values) to ensure data integrity at every stage and detects error early. It is scalable and flexible leveraging the compute power of dataware houses like Bigquery, Redshift, Snowflake and  others.
The real world transactional data for BeejanRide sits in a Postgres Database, to clean and tranform this database reguires data ingestion from the Progres database to BigQuery. By implememnting Airbyte, the transactional data is ingested into Bigquery.

## BeejanRide Entity Relationship Diagram (ERD)

<img width="332" height="390" alt="image" src="https://github.com/user-attachments/assets/9c9733a0-5e11-4610-96ef-bf0ec65c87c1" />


## BeejanRide End to End Data Flow
### Source system (Postgres database) -
The real world transactional data that powers the BeejanRide originate and sit on this Platform. The data is raw, unprocessed and does not support analytics.The source system platform supports:
  * Trip services
  * Payment services
  * Driver services
  * Riders services
  * City services
  * Driver status services
### The Raw Layer (Bigquery)
The raw data from the source system is ingested into  Bigquery, leveraging BigQuery compute power for big data, to preserve source of truth and allow reprocessing. Using Airbyte for the ingestion, the configuration on Airbyte is set to align Progres database source schema to raw layer schema and append- dedupe. The append dedupe limits the amount of data that needs to be ingested during each ingestion run, therefore improving runtime warehouse performance.

### BeejanRide Staging Layer (Dbt)
At this stage, the raw data is transformed into clean and standardize models to ensure consistent building block for the downstream analytics. The transformation process for this project includes the following: 
  * Renaming columns to snake_case
  * Casting correct data types - i.e cast(launch_date as timestamp) as launch_date
  * Deduplicate using primary keys -  using primary keys and macro for deduplication logic
  * Standardize timestamps - i.e converts datetime to timestamp
  * Remove invalid or null primary keys
  * Source definitions - create a source.yml that includes tables and column description, config freshness check for the all table. The freshness configuration check is set to check data freshness every 6 hours except for the trip_raw set for every 2 hours to ensure trip updates are captured in time.  Column level tests are performed to ensure uniqueness, accepted values and eliminate null values here applicable.

### BeejanRide Intermediate Layer (Dbt)
This is the stage where business logic are implemented by building on the staging models to create reuseable tranformation logics. The staging models and intermediate models are joined to enrich data, calculate metrics(i.e trip duration, waiting time, net revunue), identify anomalies like duplicate payments, failed payments.., and enhance reusable logics( lifetime values, fraud indicators, corporate trip flag. The business rules are centralized in the intermediate layer to ensure consistency across all  marts.
Below are the business logics required to achieve BeejanRide objectives:
  * int_driver_lifetime_trips - Built on stg_trip_raw to calculate completed driver lifetime trips  
  * int_duplicate_trip_payment - Built on stg_payments_raw to calculate successful payment and flag duplicate payments
  * int_failed _payment_surge - Built on stg_payments_raw and int_trip_duration to calculate failed payment and extreme surge multiplier.
  * int_fraud_indicators - Built on int_trip_duration which already includes duration, wait time, corporate flag logics. This model calculates various fraud indicators for each trip based on the trip duration, surge multiplier, payment method, and other factors. It serves as an intermediate layer for further analysis on potential fraudulent activities and risk assessment of trips.
  * int_net_revenue - Built on stg_payments_raw, this model calculates the net revenue for each trip by subtracting the payment fee from the total amount paid. It serves as an intermediate layer for further analysis on revenue performance and profitability of trips.
  * int_rider_lifetime_value - Built on stg_trips_raw, this model calculates the lifetime value of riders based on their completed trips.
  * int_trip_duration - Built on stg_trips_raw,  this model calculates the trip duration and wait time for each trip, as well as flags for completed and cancelled trips. It serves as an intermediate layer for further analysis on trip performance and driver/rider behavior.
It also includes a corporate_trip_flag to identify trips that were booked as corporate rides, which may have different characteristics and performance metrics compared to regular trips.

### BeejanRide Mart Layer (Dbt)
### Design Decison - Source → Raw → Staging → Intermediate → Mart → →
### Trade-off
  * Append -dedupe
  * Incremental materialization - The implementation of incremental materialization has been applied to the facts tables which includes the fact_trips, fact_payments and fact_driver_status_events. This materialization limits the amount of data that needs to be tranformed and processed for very update and during runtime. It improves warehouse query performance and reduces compute costs.

### Future Improvements


Architecture diagram
ERD
Data flow explanation
Design decisions
Tradeoffs
Future improvements

Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
