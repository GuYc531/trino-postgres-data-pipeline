with launch_year_table as (
select
success, id,
EXTRACT(YEAR FROM DATE_PARSE(REPLACE(date_utc, 'Z', ''),'%Y-%m-%dT%H:%i:%s.%f') AT TIME ZONE 'UTC') AS launch_year
from LAUNCHES_TABLE_NAME)
select
count(id) as total_launches,
SUM(CASE WHEN success = TRUE THEN 1 ELSE 0 END) as successful_launches,
(SUM(CASE WHEN success = TRUE THEN 1 ELSE 0 END) *100) / count(id) AS launches_success_rate,
launch_year
from launch_year_table
group by launch_year
order by launch_year