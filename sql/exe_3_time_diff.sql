select
AVG(EXTRACT(EPOCH FROM (la.date_utc::timestamp - la.static_fire_date_utc::timestamp)) / 3600) AS average_delay_hours,
MAX(EXTRACT(EPOCH FROM (la.date_utc::timestamp - la.static_fire_date_utc::timestamp)) / 3600) AS max_delay_hours,
EXTRACT(YEAR FROM date_utc::timestamp) AS launch_year
from {LAUNCHES_TABLE_NAME} la
group by launch_year
order by launch_year desc