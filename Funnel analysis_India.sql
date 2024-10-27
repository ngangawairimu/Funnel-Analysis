-- Step 1: Calculate the total number of events for each country and select the top 3 countries with the highest counts
WITH country_event_counts AS (
    SELECT
        country,
        COUNT(*) AS total_events -- Count the total events recorded for each country
    FROM `tc-da-1.turing_data_analytics.raw_events`
    GROUP BY country -- Group results by country to aggregate event counts
    ORDER BY total_events DESC -- Order countries by total event counts in descending order
    LIMIT 3 -- Limit the results to the top 3 countries with the most events
),

-- Step 2: Extract unique user-event combinations along with their earliest occurrence timestamp
no_duplicated_query AS (
    SELECT
        DISTINCT user_pseudo_id, -- Select unique user identifiers
        event_name AS event_type, -- Rename event_name to event_type for clarity
        MIN(event_timestamp) OVER (PARTITION BY event_name, user_pseudo_id) AS earliest_event -- Determine the earliest timestamp for each event type per user
    FROM `tc-da-1.turing_data_analytics.raw_events`
),

-- Step 3: Join the unique events with the original events table to enrich the data with country information
enriched_query AS (
    SELECT 
        nq.user_pseudo_id, -- User ID from the no_duplicated_query
        nq.event_type, -- Event type from no_duplicated_query
        bt.country, -- Country from the original raw_events
        bt.event_timestamp -- Timestamp of the event from the original raw_events
    FROM no_duplicated_query nq
    JOIN `tc-da-1.turing_data_analytics.raw_events` bt
    ON bt.event_timestamp = nq.earliest_event -- Match the earliest event timestamp
    AND nq.user_pseudo_id = bt.user_pseudo_id -- Match user IDs between both tables
    AND nq.event_type = bt.event_name -- Match event types between both tables
    WHERE bt.country IN ( -- Filter for only those countries that are in the top 3 from country_event_counts
        SELECT country
        FROM country_event_counts
    )
)

-- Step 4: Aggregate and count the occurrences of specified event types in 'India'
SELECT 
    event_type, -- The type of event (e.g., 'first_visit', 'page_view')
    COUNT(*) AS event_count -- Count how many times each event type occurred
FROM enriched_query
WHERE 
    country = 'India' -- Filter for events specifically in India
    AND event_type IN ('first_visit', 'page_view', 'purchase', 'select_item', 'view_item', 'add_payment', 'add_shipment_info', 'add_to_cart') -- Include only specified event types
GROUP BY event_type -- Group results by event type for aggregation
ORDER BY event_type; -- Sort the results alphabetically by event type
