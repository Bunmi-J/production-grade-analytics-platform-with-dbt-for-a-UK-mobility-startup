# Production-Grade Analytics Platform with dbt
BeejanRide, a UK mobility startup operating in five cities with prospect of scaling within the next couple of years. This startup is keen to take advantage of modern data stack tool dbt and requested the services of a data engineer to implement dbt to enable production grade analytics platform to power analytics, business operations, finance and timely fraud detection.

 BeejanRide provides the following services:
   * Ride-hailing
   * Airport transfers
   * Scheduled corporate rides.
     
Dbt provides a simplified environment for SQL/python tranformation logic in a modular approach that enhances business operations, logics and governance. It features built-in testing and data quality with native test (not_null, unique, accepted values) to ensure data integrity at every stage and detects error early. It is scalable and flexible leveraging the compute power of dataware houses like Bigquery, Redshift, Snowflake and  others.
The real world transactional data for BeejanRide sits in a Postgres Database, to clean and tranform this database reguires data ingestion from the Progres database to BigQuery. By implememnting Airbyte, the transactional data is ingested into Bigquery.

## BeejanRide Architecture Diagram

![beejanride_architecture_diagram](https://github.com/user-attachments/assets/4dc2f72e-61ae-4c14-b204-df82a520a98e)



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
This is the stage where dimension and fact tables are deisgned for business intelligence tools (Power BI, Tableau), Analysts and dashboards .
#### Dimensions
 Dimension table are neccessary for filtering and slicing the models. Beejan has the following dimensions.
  * Dim_riders - This model creates a dimension table for riders, which includes rider_id, country, referral_code signup_date, and created_at. It serves as a reference for other models that need to join with rider information, such as trips or payments. The data is sourced from the stg_riders_raw staging table, which is expected to have been cleaned and deduplicated in the staging layer.
    
  * Dim_drivers - This model creates a dimension table for drivers, containing relevant information about each driver.
  * Dim_payments - This model creates a dimension table for payments, which includes payment_id, trip_id, amount, fee, currency, payment_provider, payment_status, and created_at. It serves as a reference for other models that need to join with payment information, such as trips or riders. The data is sourced from the stg_payments_raw staging table, which has been cleaned and deduplicated in the staging layer.
  * Dim_cities - This model creates a dimension table for cities, which includes city_id, city_name, country, and launch_date. It serves as a reference for other models that need to join with city information, such as trips or drivers. The data is sourced from the stg_cities_raw staging table, which is has been cleaned and deduplicated in the staging layer.
  * Dim_driver_status_events - This model creates a dimension table for drivers, containing relevant information about each driver status.
#### Fact Tables
Fact table stores each or individual transactional event by that forms the real world data for analysis.
  * Fact_trips - This model creates the fact_trips table which contains detailed information about each trip, including trip duration, net revenue, surge pricing, and various flags for failed payments, duplicate payments, and fraud indicators. It joins the raw trips data with intermediate tables that calculate these metrics and flags.
  * Fact_Payments - This model creates the fact_payments table which contains payment information for each trip, along with flags for failed payments, extreme surge pricing, and duplicate payments. It joins the raw payments data with the intermediate tables that flag failed payments and duplicates.
  * Fact_driver_status_events - This model creates the fact_driver_status_events table which captures all the status events of drivers.
### Testing and Data Quality Layer.
Testing is to ascertain that the tranformed data adheres to business rules, logics while enhancing data integrity and high quality data that meets desired business objectives. Three types of tests are implemeted in BeejanRide startup.
  * Generic Test - Dbt provides native test to check Table columns for uniqueness, not null values,accepted_values and relationshiops e.tc.The description of the tables, columns and tests are all defined in the source and  schema.yml files. Execution of Dbt test lets you know if the test is successful or not, by showing error messages for failed test.
    
  * Customs Test- This is customized test defined by the data owner to achieve certain business objectives. SQL files are created and defined in the test folder and dbt test is executed to check the test works. Beejanride implemented these tests to measure metrics, catch early errors and identify any fraud transactions. The following test were implemeneted:
       * No negative revenue
       * Trip duration > 0
       * Completed trip must have successful payment
  * Freshness Test - This can be configured in the source.yml file to check the freshness of the data.
### Documentation and Governance
To ensure smooth operations and maintainability, the models are tagged for operational efficiency
- Tags (finance, operations, fraud)
- Owner metadata - 
- Descriptions -
- dbt docs - dbt docs are generated by executing "dbt docs generate", - this generate a catalog.json. under the target folder. And by running "dbt docs serve" to open up a web browser version of the documentation.
- Lineage graph - Shows the model dependencies

    
### Design Decison - Source → Raw → Staging → Intermediate → Mart → →

### Trade-off
  * Append -dedupe- this setting ensures only updates are merged/ingested into the 
 datawarehouse  by using the primary key to deduplicate duplicates data.
 * Incremental materialization - The implementation of incremental materialization has been applied to the following facts tables, which include the fact_trips, fact_payments and fact_driver_status_events. This materialization limits the amount of data that needs to be tranformed and processed for very update and during runtime. It improves warehouse query performance and reduces compute costs.

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
