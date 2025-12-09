--select
--AVG(EXTRACT(EPOCH FROM (cast(la.date_utc as timestamp) - cast(la.static_fire_date_utc as timestamp))) / 3600) AS average_delay_hours,
--MAX(EXTRACT(EPOCH FROM (cast(la.date_utc as timestamp) - cast(la.static_fire_date_utc as timestamp))) / 3600) AS max_delay_hours,
--EXTRACT(YEAR FROM cast(date_utc as timestamp)) AS launch_year
--from LAUNCHES_TABLE_NAME la
--group by launch_year
--order by launch_year desc
with no_time_zone as (
SELECT
    REPLACE(static_fire_date_utc, 'Z', '') AS static_fire_date_utc,
    REPLACE(date_utc, 'Z', '') AS date_utc
from LAUNCHES_TABLE_NAME
)
select
extract(epoch from (DATE_PARSE(static_fire_date_utc,'%Y-%m-%dT%H:%i:%s.%f') AT TIME ZONE 'UTC')) as static_fire_date_utc_time,
extract(epoch from (DATE_PARSE(date_utc,'%Y-%m-%dT%H:%i:%s.%f') AT TIME ZONE 'UTC')) as date_utc_time
--AVG(EXTRACT(EPOCH FROM (cast(la.date_utc as timestamp) - cast(la.static_fire_date_utc as timestamp))) / 3600) AS average_delay_hours,
--MAX(EXTRACT(EPOCH FROM (cast(la.date_utc as timestamp) - cast(la.static_fire_date_utc as timestamp))) / 3600) AS max_delay_hours,
--EXTRACT(YEAR FROM cast(date_utc as timestamp)) AS launch_year
from no_time_zone
--group by launch_year
--order by launch_year desc
