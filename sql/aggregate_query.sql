SELECT
    COUNT(DISTINCT la.id) AS total_launches,
    SUM(CASE WHEN la.success IS TRUE THEN 1 ELSE 0 END) AS successful_launches,
    AVG(pa.mass_kg) AS average_payload_mass_kg,
    AVG(EXTRACT(EPOCH FROM (la.date_utc::timestamp - la.static_fire_date_utc::timestamp)) / 3600) AS Average_delay_between_scheduled_and_actual_launch_times_hours,
    now() as insert_time
FROM
    LAUNCHES_TABLE_NAME la
LEFT JOIN
    PAYLOADS_TABLE_NAME pa
ON
    la.payloads_0 = pa.id