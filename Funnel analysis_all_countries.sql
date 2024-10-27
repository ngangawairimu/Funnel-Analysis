-- Step 1: Count events by country and select the top 3 countries with the highest number of events
WITH country_event_counts AS (
    SELECT
        country,
        COUNT(*) AS total_events
    FROM `tc-da-1.turing_data_analytics.raw_events`
    GROUP BY country
    ORDER BY total_events DESC
    LIMIT 3
),

-- Step 2: Extract unique user and event combinations along with the earliest timestamp of each event type for each user
no_duplicated_query AS (
    SELECT
        DISTINCT user_pseudo_id,
        event_name AS event_type,
        MIN(event_timestamp) OVER (PARTITION BY event_name, user_pseudo_id) AS earliest_event
    FROM `tc-da-1.turing_data_analytics.raw_events`
),

-- Step 3: Join the earliest events with the original events table to add country data and filter by top countries
enriched_query AS (
    SELECT 
        nq.user_pseudo_id,
        nq.event_type,
        bt.country,
        bt.event_timestamp
    FROM no_duplicated_query nq
    JOIN `tc-da-1.turing_data_analytics.raw_events` bt
    ON bt.event_timestamp = nq.earliest_event
    AND nq.user_pseudo_id = bt.user_pseudo_id
    AND nq.event_type = bt.event_name
    WHERE bt.country IN (
        SELECT country
        FROM country_event_counts
    )
)

-- Step 4: Aggregate the event counts for specified event types in the top countries and sort by event type
SELECT 
    event_type,
    COUNT(*) AS total_event_count
FROM enriched_query
WHERE 
    country IN ('United States', 'India', 'Canada')
    AND event_type IN ('first_visit', 'page_view', 'purchase', 'select_item', 'view_item', 'add_payment', 'add_shipment_info', 'add_to_cart')
GROUP BY event_type 
ORDER BY event_type;
