with total_payload as (
select la.id,
la.name,
la.launchpad,
 coalesce(pa.mass_kg, 0) + coalesce(pa2.mass_kg,0) as total_mass_kg
from LAUNCHES_TABLE_NAME la
left join PAYLOADS_TABLE_NAME pa
on pa.id = la.payloads_0
left join PAYLOADS_TABLE_NAME pa2
on pa2.id = la.payloads_1)
select launchpad,
count(id) as total_launches,
 AVG(total_mass_kg) as average_payload
from total_payload
group by launchpad