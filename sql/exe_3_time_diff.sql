with no_time_zone as (
SELECT
    DATE_PARSE(REPLACE(static_fire_date_utc, 'Z', ''),'%Y-%m-%dT%H:%i:%s.%f') AT TIME ZONE 'UTC' AS static_fire_date_utc,
    DATE_PARSE(REPLACE(date_utc, 'Z', ''),'%Y-%m-%dT%H:%i:%s.%f') AT TIME ZONE 'UTC' AS date_utc
from LAUNCHES_TABLE_NAME
)
select
avg(date_diff('HOUR', static_fire_date_utc ,date_utc )) as average_delay_hours,
max(date_diff('HOUR', static_fire_date_utc ,date_utc )) as max_delay_hours,
EXTRACT(YEAR FROM (date_utc)) AS launch_year
from no_time_zone
group by EXTRACT(YEAR FROM (date_utc))
order by EXTRACT(YEAR FROM (date_utc))  desc
