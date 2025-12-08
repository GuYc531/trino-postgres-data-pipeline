select
count(id) as total_launches,
SUM(CASE WHEN success IS TRUE THEN 1 ELSE 0 END) as successful_launches,
(SUM(CASE WHEN success IS TRUE THEN 1 ELSE 0 END) *100) / count(id) AS launches_success_rate,
EXTRACT(YEAR FROM date_utc::timestamp) AS launch_year
from {LAUNCHES_TABLE_NAME}
group by launch_year
order by launch_year