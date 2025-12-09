with summed_mass as (
SELECT
        la.id,
        la.name,
        cast((coalesce(pa.mass_kg, 0) + coalesce(pa2.mass_kg,0)) as DOUBLE) as total_mass
    FROM LAUNCHES_TABLE_NAME la
    LEFT JOIN PAYLOADS_TABLE_NAME pa
    ON pa.id = la.payloads_0
    LEFT JOIN PAYLOADS_TABLE_NAME pa2
    ON pa2.id = la.payloads_1),
    ranked_launches as (
    select id, name, total_mass,
    DENSE_RANK() OVER (ORDER BY total_mass DESC) as mass_rank
    from summed_mass)
SELECT
    id,
    name,
    total_mass
FROM ranked_launches
WHERE mass_rank <= 5
ORDER BY mass_rank, total_mass DESC, id