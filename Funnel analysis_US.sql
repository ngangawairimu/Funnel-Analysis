-- Step 1: Calculate the total events per country and select the top 3 countries with the highest event counts
WITH country_event_counts AS (
    SELECT
        country,
        COUNT(*) AS total_events -- Count the total number of events for each country
    FROM `tc-da-1.turing_data_analytics.raw_events`
    GROUP BY country -- Group by country to aggregate event counts by country
    ORDER BY total_events DESC -- Order countries by event count in descending order
    LIMIT 3 -- Select only the top 3 countries with the highest event counts
),

-- Step 2: Identify unique user-event combinations and their earliest event timestamp
no_duplicated_query AS (
    SELECT
        DISTINCT user_pseudo_id, -- Unique identifier for each user
        event_name AS event_type, -- Rename event_name to event_type for consistency in naming
        MIN(event_timestamp) OVER (PARTITION BY event_name, user_pseudo_id) AS earliest_event -- Get the earliest timestamp for each event type per user
    FROM `tc-da-1.turing_data_analytics.raw_events`
),

-- Step 3: Join the no_duplicated_query table with raw_events to add country information 
-- for the earliest events in the top countries
enriched_query AS (
    SELECT 
        nq.user_pseudo_id, -- User ID from the no_duplicated_query table
        nq.event_type, -- Event type from no_duplicated_query table
        bt.country, -- Country associated with the event from raw_events table
        bt.event_timestamp -- Event timestamp from raw_events table
    FROM no_duplicated_query nq
    JOIN `tc-da-1.turing_data_analytics.raw_events` bt
    ON bt.event_timestamp = nq.earliest_event -- Match on the earliest event timestamp for each event type per user
    AND nq.user_pseudo_id = bt.user_pseudo_id -- Match user IDs between the two tables
    AND nq.event_type = bt.event_name -- Match event types between the two tables
    WHERE bt.country IN ( -- Filter only for countries in the top 3 from country_event_counts
        SELECT country
        FROM country_event_counts
    )
)

-- Step 4: Aggregate event counts for specific event types in the 'United States' and sort by event type
SELECT 
    event_type, -- The type of event (e.g., 'first_visit', 'page_view')
    COUNT(*) AS event_count -- Count the occurrences of each event type
FROM enriched_query
WHERE 
    country = 'United States' -- Filter for events in the United States
    AND event_type IN ('first_visit', 'page_view', 'purchase', 'select_item', 'view_item', 'add_payment', 'add_shipment_info', 'add_to_cart') -- Include only specified event types
GROUP BY event_type -- Group results by each event type
ORDER BY event_type; -- Sort results alphabetically by event type
