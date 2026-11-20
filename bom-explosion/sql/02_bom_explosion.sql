-- BoM explosion: for every FIN material, each material -> component pair of its
-- production chain, by plant, FIN material and year, with quantities for the year.
-- The same logic as the notebook.

DROP VIEW IF EXISTS bom_explosion;

CREATE VIEW bom_explosion AS
WITH RECURSIVE

-- A material's produced quantity is on each of its component rows,
-- so it's taken once a month and then summed over the year.
production AS (
    SELECT plant_id, year, produced_material, sum(quantity) AS produced_quantity
    FROM (
        SELECT
            plant_id,
            year,
            month,
            produced_material,
            max(produced_material_quantity) AS quantity
        FROM bom
        GROUP BY plant_id, year, month, produced_material
    ) AS monthly
    GROUP BY plant_id, year, produced_material
),

-- The pairs for the whole year, per plant.
pairs AS (
    SELECT
        plant_id,
        year,
        produced_material,
        produced_material_release_type,
        produced_material_production_type,
        component_material,
        component_material_release_type,
        component_material_production_type,
        sum(component_material_quantity) AS consumed_quantity
    FROM bom
    GROUP BY
        plant_id,
        year,
        produced_material,
        produced_material_release_type,
        produced_material_production_type,
        component_material,
        component_material_release_type,
        component_material_production_type
),

annual AS (
    SELECT pairs.*, production.produced_quantity
    FROM pairs
    JOIN production USING (plant_id, year, produced_material)
),

-- Walks down from each FIN material a level at a time. Level 1 is the pairs of the
-- PROD material the FIN consists of, level 2 the pairs of that material's PROD
-- components, and so on until only ADD and RM are left.
chain AS (
    SELECT
        fin.plant_id,
        fin.year,
        fin.produced_material                 AS fin_material_id,
        fin.produced_material_release_type    AS fin_material_release_type,
        fin.produced_material_production_type AS fin_material_production_type,
        fin.produced_quantity                 AS fin_production_quantity,
        1                                     AS level,
        pair.produced_material,
        pair.produced_material_release_type,
        pair.produced_material_production_type,
        pair.produced_quantity,
        pair.component_material,
        pair.component_material_release_type,
        pair.component_material_production_type,
        pair.consumed_quantity
    FROM annual AS fin
    JOIN annual AS pair
        ON  pair.plant_id = fin.plant_id
        AND pair.year = fin.year
        AND pair.produced_material = fin.component_material
    WHERE fin.produced_material_release_type = 'FIN'

    UNION ALL

    SELECT
        chain.plant_id,
        chain.year,
        chain.fin_material_id,
        chain.fin_material_release_type,
        chain.fin_material_production_type,
        chain.fin_production_quantity,
        chain.level + 1,
        pair.produced_material,
        pair.produced_material_release_type,
        pair.produced_material_production_type,
        pair.produced_quantity,
        pair.component_material,
        pair.component_material_release_type,
        pair.component_material_production_type,
        pair.consumed_quantity
    FROM chain
    JOIN annual AS pair
        ON  pair.plant_id = chain.plant_id
        AND pair.year = chain.year
        AND pair.produced_material = chain.component_material
)
-- if the data has a cycle, the walk stops at a material it has already been through
CYCLE produced_material SET is_cycle USING path

SELECT
    plant_id                          AS plant,
    fin_material_id,
    fin_material_release_type,
    fin_material_production_type,
    fin_production_quantity,
    produced_material                 AS prod_material_id,
    produced_material_release_type    AS prod_material_release_type,
    produced_material_production_type AS prod_material_production_type,
    produced_quantity                 AS prod_material_production_quantity,
    component_material                AS component_id,
    component_material_release_type,
    component_material_production_type,
    consumed_quantity                 AS component_consumption_quantity,
    year
FROM chain
WHERE NOT is_cycle
ORDER BY plant_id, fin_material_id, year, level, produced_material, component_material;
