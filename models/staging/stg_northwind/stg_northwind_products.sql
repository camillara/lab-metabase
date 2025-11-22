{{ config(materialized = 'view') }}

WITH source AS (
    SELECT
        p.product_id                         AS source_product_id,
        UPPER(TRIM(p.product_name))          AS product_name,
        p.category_id                        AS category_id,
        UPPER(TRIM(c.category_name))         AS category_name,
        p.supplier_id                        AS supplier_id,
        UPPER(TRIM(s.company_name))          AS vendor_name,
        TRIM(p.quantity_per_unit)            AS quantity_per_unit,
        p.unit_price::numeric(12,2)          AS unit_price,
        p.units_in_stock                     AS units_in_stock,
        p.units_on_order                     AS units_on_order,
        p.reorder_level                      AS reorder_level,
        p.discontinued                       AS discontinued,
        'northwind'                          AS source_system
    FROM ext_northwind.products  p
    LEFT JOIN ext_northwind.categories c
           ON p.category_id = c.category_id
    LEFT JOIN ext_northwind.suppliers s
           ON p.supplier_id = s.supplier_id
)

SELECT
    source_product_id,
    product_name,
    category_id,
    category_name,
    supplier_id,
    vendor_name,
    quantity_per_unit,
    unit_price,
    units_in_stock,
    units_on_order,
    reorder_level,
    discontinued,
    source_system
FROM source
