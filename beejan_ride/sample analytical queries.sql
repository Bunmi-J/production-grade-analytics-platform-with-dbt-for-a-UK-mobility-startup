Sample analytical queries demonstrating insights
1a. Total Corporate revenue by city

select
    c.city_name,
    count(t.is_corporate) as total_corporate_trip,
    sum(t.net_revenue_calc) as corporate_revenue
from beejanride-488423.beejan_dataset.fact_trips t
join beejanride-488423.beejan_dataset.dim_riders r
    on t.rider_id = r.rider_id
join beejanride-488423.beejan_dataset.dim_cities c
    on t.city_id = c.city_id
where t.is_corporate = true
group by city_name
order by total_corporate_trip desc;


1.Completed Trip with Failed Payments - Fraud, Finance


select ct.trip_id, ct.status,fp.payment_status
from beejan_dataset.fact_trips ct
left join beejan_dataset.fact_payments fp
    on ct.trip_id = fp.trip_id
where ct.dropoff_at is not null
  and (fp.trip_id is null or fp.payment_status != 'success')


2. Trip with high surge multiplier -Fraud Flags for finance- prioritize fraud investigation.

select
    trip_id,
    rider_id,
    driver_id,
    actual_fare,
    surge_multiplier,
    high_surge_flag,
    net_revenue_calc
from beejan_dataset.fact_trips
where high_surge_flag = 1
order by surge_multiplier desc

3. Drivers total earning- Understand driver contribution to the platform.
select 
    driver_id,
    sum(net_revenue_calc) as driver_total_earning,
    driver_lifetime_trips
from `beejanride-488423.beejan_dataset.fact_trips`
group by driver_id, driver_lifetime_trips
order by driver_lifetime_trips desc;

4. check duplicate payments - Fraud, Finance- identify overcharges or system error
select
    payment_id,
    trip_id,
    amount,
    duplicate_payment_flag
from beejanride-488423.beejan_dataset.fact_payments
where duplicate_payment_flag = 1

5. failed payment
select
    t.trip_id,
    t.surge_multiplier,
    p.payment_status,
    t.failed_payment_on_completed_trip_flag
from beejanride-488423.beejan_dataset.fact_trips t
join beejanride-488423.beejan_dataset.fact_payments p
    on t.trip_id = p.trip_id
where t.failed_payment_on_completed_trip_flag = 1
order by surge_multiplier desc;


4. Drivers status - operations
select
    driver_id,
    event_timestamp,
    status
from `beejan_dataset.fact_driver_status_events`
order by event_timestamp desc;

