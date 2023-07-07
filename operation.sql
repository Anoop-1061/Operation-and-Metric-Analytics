create database operation;

use operation;

select * from users;

describe users;

ALTER TABLE users
MODIFY COLUMN created_at datetime;

CREATE TABLE events (
    user_id INT,
    occurred_at DATETIME,
    event_type VARCHAR(30),
    event_name VARCHAR(30),
    location CHAR(25),
    device VARCHAR(30)
);

load data infile 'S:/Trainity/Operation Analytics and Investigating Metric Spike/Table-2 events.csv' into table events
fields terminated by ','
ignore 1 lines;

select * from events;

CREATE TABLE email_events (
    user_id INT,
    occurred_at DATETIME,
    action VARCHAR(30),
    user_type INT
);

load data infile 'S:/Trainity/Operation Analytics and Investigating Metric Spike/Table-3 email_events.csv' into table email_events
fields terminated by ','
ignore 1 lines;

select * from email_events;

select * from jobs;

ALTER TABLE jobs
MODIFY COLUMN ds date;

-- number of jobs reviewed per hour per day for November 2020?

SELECT 
    ds as date,
    COUNT(*) AS job_reviewed,
    sum(time_spent) / 3600 AS time_spent_per_hour
FROM
    jobs
WHERE
    ds >= '2020-11-01' AND ds < '2020-12-01'
GROUP BY date;

SELECT ds, event, count(event) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_average
FROM jobs;

-- Calculate 7 day rolling average of throughput

CREATE TEMPORARY TABLE JOBS_REVIEWED
(
SELECT ds,count(distinct job_id) as jobs_reviewed,CAST(COUNT(DISTINCT JOB_ID)/86400 AS DECIMAL(10,10)) AS THROUGHPUT 
	from jobs
    group by ds
    order by ds 
);
SELECT ds,jobs_reviewed,throughput
,avg(throughput) OVER(ORDER BY DS ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS throughput_7day
FROM JOBS_REVIEWED;

SELECT ds, AVG(events_per_day) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT
ROW) as rolling_avg_throughput
FROM (
	SELECT ds, COUNT(event) as events_per_day
	FROM jobs
	GROUP BY ds
) a;

-- Percentage share of each language: Share of each language for different contents in last 30 days.

SELECT 
    language, count(job_id) as jobs_applied,
    COUNT(*) / (SELECT COUNT(*) FROM jobs WHERE ds >= '2020-11-01' AND ds < '2020-12-01') * 100 
    AS percentage_share_of_language
FROM
    jobs
WHERE
    ds >= '2020-11-01' AND ds < '2020-12-01'
GROUP BY language;

-- display of duplicate rows

select * from
(
select *,
row_number() over (partition by job_id order by ds) as rownum
from jobs
)as t
where rownum>1;

-- Weekly User Engagement

SELECT 
    user_id,
    EXTRACT(WEEK FROM occurred_at) AS week_num,
    COUNT(event_type) AS total_events
FROM
    events
WHERE
    event_type = 'engagement'
GROUP BY user_id , week_num
ORDER BY user_id;

SELECT
    user_id,
    sum(event_name) as total_events_per_user,
    AVG(event_name) AS average_no_of_events_per_user_per_week
FROM (
    SELECT
        user_id,
        COUNT(event_name) AS event_name
    FROM events
    WHERE
		event_type='engagement'
    GROUP BY user_id, week(occurred_at)
) subquery
GROUP BY user_id
ORDER BY user_id;

-- Calculate User Growth rate: Amount of users growing over month for a product.

SELECT
    extract(month from occurred_at) AS month,
    COUNT(*) AS new_users,
    LAG(COUNT(*)) OVER (ORDER BY extract(month from occurred_at)) AS previous_month_users,
    
    (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY extract(month from occurred_at))) / 
    LAG(COUNT(*)) OVER (ORDER BY extract(month from occurred_at)) * 100 AS growth_rate
FROM
    events
WHERE
    occurred_at >= '2014-01-01' 
    AND occurred_at< '2015-01-01' 
GROUP BY
    extract(month from occurred_at)
ORDER BY
    extract(month from occurred_at);
    
-- Calculate User Growth rate: Amount of users growing over month and week for a product.
    
SELECT
    extract(month from occurred_at) AS month, extract(week from occurred_at) as week,
    COUNT(*) AS new_users,
    LAG(COUNT(*)) OVER (ORDER BY extract(month from occurred_at)) AS previous_new_users,
    (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY extract(month from occurred_at))) / 
    LAG(COUNT(*)) OVER (ORDER BY extract(month from occurred_at)) * 100 AS growth_rate
FROM
    events
WHERE
    occurred_at >= '2014-01-01' 
    AND occurred_at< '2015-01-01'
GROUP BY
    extract(month from occurred_at), extract(week from occurred_at)
ORDER BY
    extract(month from occurred_at);

-- Weekly Retention rate: Users engaging product weekly after signing-up for a product

SELECT
    signup_week,
    cohort_size,
    retained_users,
    (retained_users / cohort_size) * 100 AS retention_rate
FROM (
    SELECT
        signup_week,
        COUNT(DISTINCT user_id) AS cohort_size,
		count(DISTINCT CASE WHEN signup_week < activity_week AND event_type = 'signup_flow' 
        and event_name='complete_signup' then user_id END) AS signup__users,
        count(DISTINCT CASE WHEN activity_week >= signup_week AND event_type = 'engagement' 
        and event_name='login' then user_id END) AS retained_users
    FROM (
        SELECT
            user_id,
            event_type, event_name,
            extract(week from occurred_at) AS signup_week, 
            extract(week from occurred_at) AS activity_week
        FROM
            events
    ) sub1
    GROUP BY
        signup_week
) sub2
ORDER BY
    signup_week;
    
-- Calculate the weekly engagement per device?

SELECT 
    device,
    EXTRACT(WEEK FROM occurred_at) AS week_num,
    COUNT(event_name) AS total_events
FROM
    events
WHERE
    event_type = 'engagement'
GROUP BY device , week_num;

SELECT
    device,
    sum(event_name) as total_events_per_device,
    AVG(event_name) AS average_no_of_events_per_device_per_week
FROM (
    SELECT
        device,
        COUNT(event_name) AS event_name
    FROM events
    where 
		event_type='engagement'
    GROUP BY device, week(occurred_at)
) subquery
GROUP BY device;

-- Email engagement Metrics

SELECT
    ee.action,
    COUNT(ee.action) AS event_count,
    COUNT(*) / (select count(*) from email_events) * 100 AS percentage_share
FROM
    email_events ee
GROUP BY
    ee.action;

SELECT
    ee.action, week(occurred_at),
    COUNT(ee.action) AS event_count
FROM
    email_events ee
GROUP BY
    ee.action, week(occurred_at)
order by 
	ee.action;