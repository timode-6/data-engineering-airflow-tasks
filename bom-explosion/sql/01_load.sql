-- Loads the BoM export, data/bom.csv, into the bom table.
-- Run it with psql from the project folder: \copy reads the file relative to it.

BEGIN;

-- The export is text with thousands separators in the quantities ("1,726.00"),
-- so it goes into an all-text table first and gets its types on the way to bom.
CREATE TEMP TABLE bom_csv (
    year                               text,
    month                              text,
    produced_material                  text,
    produced_material_production_type  text,
    produced_material_release_type     text,
    produced_material_quantity         text,
    component_material                 text,
    component_material_production_type text,
    component_material_release_type    text,
    component_material_quantity        text,
    plant_id                           text
) ON COMMIT DROP;

-- HEADER MATCH: the file must have exactly these columns, in this order
\copy bom_csv FROM 'data/bom.csv' WITH (FORMAT csv, HEADER MATCH)

DROP TABLE IF EXISTS bom CASCADE;  -- the view goes too, 02 creates it again

CREATE TABLE bom (
    year                               integer NOT NULL,
    month                              integer NOT NULL CHECK (month BETWEEN 1 AND 12),
    produced_material                  text    NOT NULL,
    produced_material_production_type  text,
    -- only FIN and PROD materials have components
    produced_material_release_type     text    NOT NULL
        CHECK (produced_material_release_type IN ('FIN', 'PROD')),
    produced_material_quantity         numeric NOT NULL,
    component_material                 text    NOT NULL,
    component_material_production_type text,
    component_material_release_type    text    NOT NULL
        CHECK (component_material_release_type IN ('FIN', 'PROD', 'ADD', 'RM')),
    component_material_quantity        numeric NOT NULL,
    plant_id                           text    NOT NULL,
    PRIMARY KEY (plant_id, year, month, produced_material, component_material)
);

INSERT INTO bom
SELECT
    year::integer,
    month::integer,
    trim(produced_material),
    nullif(trim(produced_material_production_type), ''),
    trim(produced_material_release_type),
    replace(produced_material_quantity, ',', '')::numeric,
    trim(component_material),
    nullif(trim(component_material_production_type), ''),
    trim(component_material_release_type),
    replace(component_material_quantity, ',', '')::numeric,
    trim(plant_id)
FROM bom_csv;

COMMIT;

SELECT count(*) AS loaded_rows FROM bom;
