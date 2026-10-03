

CREATE OR REPLACE TABLE dim_customer AS

WITH

person_demographics AS (
    SELECT
        p.business_entity_id,

        NULLIF(
            regexp_extract(
                CAST(p.demographics AS VARCHAR),
                '<DateFirstPurchase>([^<]*)</DateFirstPurchase>',
                1
            ),
            ''
        ) AS date_first_purchase_raw,

        NULLIF(
            regexp_extract(
                CAST(p.demographics AS VARCHAR),
                '<BirthDate>([^<]*)</BirthDate>',
                1
            ),
            ''
        ) AS birth_date_raw,

        NULLIF(
            regexp_extract(
                CAST(p.demographics AS VARCHAR),
                '<MaritalStatus>([^<]*)</MaritalStatus>',
                1
            ),
            ''
        ) AS marital_status,

        NULLIF(
            regexp_extract(
                CAST(p.demographics AS VARCHAR),
                '<YearlyIncome>([^<]*)</YearlyIncome>',
                1
            ),
            ''
        ) AS yearly_income,

        NULLIF(
            regexp_extract(
                CAST(p.demographics AS VARCHAR),
                '<Gender>([^<]*)</Gender>',
                1
            ),
            ''
        ) AS gender,

        TRY_CAST(
            NULLIF(
                regexp_extract(
                    CAST(p.demographics AS VARCHAR),
                    '<TotalChildren>([^<]*)</TotalChildren>',
                    1
                ),
                ''
            ) AS INTEGER
        ) AS total_children,

        TRY_CAST(
            NULLIF(
                regexp_extract(
                    CAST(p.demographics AS VARCHAR),
                    '<NumberChildrenAtHome>([^<]*)</NumberChildrenAtHome>',
                    1
                ),
                ''
            ) AS INTEGER
        ) AS number_children_at_home,

        NULLIF(
            regexp_extract(
                CAST(p.demographics AS VARCHAR),
                '<Education>([^<]*)</Education>',
                1
            ),
            ''
        ) AS education,

        NULLIF(
            regexp_extract(
                CAST(p.demographics AS VARCHAR),
                '<Occupation>([^<]*)</Occupation>',
                1
            ),
            ''
        ) AS occupation,

        NULLIF(
            regexp_extract(
                CAST(p.demographics AS VARCHAR),
                '<HomeOwnerFlag>([^<]*)</HomeOwnerFlag>',
                1
            ),
            ''
        ) AS home_owner_flag_raw,

        TRY_CAST(
            NULLIF(
                regexp_extract(
                    CAST(p.demographics AS VARCHAR),
                    '<NumberCarsOwned>([^<]*)</NumberCarsOwned>',
                    1
                ),
                ''
            ) AS INTEGER
        ) AS number_cars_owned,

        NULLIF(
            regexp_extract(
                CAST(p.demographics AS VARCHAR),
                '<CommuteDistance>([^<]*)</CommuteDistance>',
                1
            ),
            ''
        ) AS commute_distance

    FROM person.person AS p
),

store_demographics AS (
    SELECT
        s.business_entity_id,

        NULLIF(
            regexp_extract(
                CAST(s.demographics AS VARCHAR),
                '<ContactName>([^<]*)</ContactName>',
                1
            ),
            ''
        ) AS contact_name,

        NULLIF(
            regexp_extract(
                CAST(s.demographics AS VARCHAR),
                '<JobTitle>([^<]*)</JobTitle>',
                1
            ),
            ''
        ) AS job_title,

        NULLIF(
            regexp_extract(
                CAST(s.demographics AS VARCHAR),
                '<BusinessType>([^<]*)</BusinessType>',
                1
            ),
            ''
        ) AS business_type,

        TRY_CAST(
            NULLIF(
                regexp_extract(
                    CAST(s.demographics AS VARCHAR),
                    '<YearOpened>([^<]*)</YearOpened>',
                    1
                ),
                ''
            ) AS INTEGER
        ) AS year_opened,

        NULLIF(
            regexp_extract(
                CAST(s.demographics AS VARCHAR),
                '<Specialty>([^<]*)</Specialty>',
                1
            ),
            ''
        ) AS specialty,

        TRY_CAST(
            NULLIF(
                regexp_extract(
                    CAST(s.demographics AS VARCHAR),
                    '<SquareFeet>([^<]*)</SquareFeet>',
                    1
                ),
                ''
            ) AS DOUBLE
        ) AS square_feet,

        NULLIF(
            regexp_extract(
                CAST(s.demographics AS VARCHAR),
                '<Brands>([^<]*)</Brands>',
                1
            ),
            ''
        ) AS brands,

        NULLIF(
            regexp_extract(
                CAST(s.demographics AS VARCHAR),
                '<Internet>([^<]*)</Internet>',
                1
            ),
            ''
        ) AS internet,

        TRY_CAST(
            NULLIF(
                regexp_extract(
                    CAST(s.demographics AS VARCHAR),
                    '<NumberEmployees>([^<]*)</NumberEmployees>',
                    1
                ),
                ''
            ) AS INTEGER
        ) AS number_employees

    FROM sales.store AS s
),

email_ranked AS (
    SELECT
        e.business_entity_id,
        NULLIF(TRIM(e.email_address), '') AS email_address,
        TRY_CAST(e.modified_date AS TIMESTAMP) AS source_modified_at

    FROM person.email_address AS e

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY e.business_entity_id
        ORDER BY
            TRY_CAST(e.modified_date AS TIMESTAMP) DESC NULLS LAST,
            TRY_CAST(e.email_address_id AS INTEGER) DESC NULLS LAST
    ) = 1
),

phone_ranked AS (
    SELECT
        pp.business_entity_id,
        NULLIF(TRIM(pp.phone_number), '') AS phone_number,
        NULLIF(TRIM(pnt.name), '') AS phone_number_type,
        TRY_CAST(pp.modified_date AS TIMESTAMP) AS source_modified_at

    FROM person.person_phone AS pp

    LEFT JOIN person.phone_number_type AS pnt
        ON pp.phone_number_type_id = pnt.phone_number_type_id

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pp.business_entity_id
        ORDER BY
            CASE LOWER(pnt.name)
                WHEN 'cell' THEN 1
                WHEN 'home' THEN 2
                WHEN 'work' THEN 3
                ELSE 4
            END,
            TRY_CAST(pp.modified_date AS TIMESTAMP) DESC NULLS LAST,
            pp.phone_number
    ) = 1
),

address_ranked AS (
    SELECT
        bea.business_entity_id,
        NULLIF(TRIM(atype.name), '') AS address_type,
        NULLIF(TRIM(a.address_line1), '') AS address_line1,
        NULLIF(TRIM(a.address_line2), '') AS address_line2,
        NULLIF(TRIM(a.city), '') AS city,
        NULLIF(TRIM(sp.state_province_code), '') AS state_province_code,
        NULLIF(TRIM(sp.name), '') AS state_province_name,
        NULLIF(TRIM(a.postal_code), '') AS postal_code,
        NULLIF(TRIM(cr.country_region_code), '') AS country_region_code,
        NULLIF(TRIM(cr.name), '') AS country_region_name,

        GREATEST(
            TRY_CAST(bea.modified_date AS TIMESTAMP),
            TRY_CAST(a.modified_date AS TIMESTAMP),
            TRY_CAST(sp.modified_date AS TIMESTAMP),
            TRY_CAST(cr.modified_date AS TIMESTAMP)
        ) AS source_modified_at

    FROM person.business_entity_address AS bea

    INNER JOIN person.address AS a
        ON bea.address_id = a.address_id

    LEFT JOIN person.address_type AS atype
        ON bea.address_type_id = atype.address_type_id

    LEFT JOIN person.state_province AS sp
        ON a.state_province_id = sp.state_province_id

    LEFT JOIN person.country_region AS cr
        ON sp.country_region_code = cr.country_region_code

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY bea.business_entity_id
        ORDER BY
            CASE LOWER(atype.name)
                WHEN 'home' THEN 1
                WHEN 'primary' THEN 2
                WHEN 'main office' THEN 3
                WHEN 'shipping' THEN 4
                WHEN 'billing' THEN 5
                ELSE 6
            END,
            TRY_CAST(bea.modified_date AS TIMESTAMP) DESC NULLS LAST,
            TRY_CAST(bea.address_id AS INTEGER) DESC NULLS LAST
    ) = 1
),

customer_source AS (
    SELECT
        TRY_CAST(c.customer_id AS INTEGER) AS customer_id,
        NULLIF(TRIM(c.account_number), '') AS account_number,
        TRY_CAST(c.person_id AS INTEGER) AS person_id,
        TRY_CAST(c.store_id AS INTEGER) AS store_id,

        CASE
            WHEN TRY_CAST(c.store_id AS INTEGER) IS NOT NULL THEN 'STORE'
            WHEN TRY_CAST(c.person_id AS INTEGER) IS NOT NULL THEN 'INDIVIDUAL'
            ELSE 'UNKNOWN'
        END AS customer_type,

        COALESCE(
            NULLIF(TRIM(s.name), ''),
            NULLIF(
                TRIM(
                    concat_ws(
                        ' ',
                        NULLIF(TRIM(p.title), ''),
                        NULLIF(TRIM(p.first_name), ''),
                        NULLIF(TRIM(p.middle_name), ''),
                        NULLIF(TRIM(p.last_name), ''),
                        NULLIF(TRIM(p.suffix), '')
                    )
                ),
                ''
            ),
            NULLIF(TRIM(c.account_number), '')
        ) AS customer_name,

        p.person_type,

        CASE CAST(p.name_style AS VARCHAR)
            WHEN '1' THEN TRUE
            WHEN '0' THEN FALSE
            WHEN 'true' THEN TRUE
            WHEN 'false' THEN FALSE
            ELSE NULL
        END AS name_style,

        NULLIF(TRIM(p.title), '') AS title,
        NULLIF(TRIM(p.first_name), '') AS first_name,
        NULLIF(TRIM(p.middle_name), '') AS middle_name,
        NULLIF(TRIM(p.last_name), '') AS last_name,
        NULLIF(TRIM(p.suffix), '') AS suffix,
        TRY_CAST(p.email_promotion AS SMALLINT) AS email_promotion,

        email.email_address,
        phone.phone_number,
        phone.phone_number_type,

        address.address_type,
        address.address_line1,
        address.address_line2,
        address.city,
        address.state_province_code,
        address.state_province_name,
        address.postal_code,
        address.country_region_code,
        address.country_region_name,

        TRY_CAST(c.territory_id AS INTEGER) AS territory_id,
        territory.name AS territory_name,
        territory."group" AS territory_group,
        territory.country_region_code AS territory_country_region_code,

        TRY_CAST(
            REPLACE(person_demographics.date_first_purchase_raw, 'Z', '')
            AS DATE
        ) AS demog__date_first_purchase,

        TRY_CAST(
            REPLACE(person_demographics.birth_date_raw, 'Z', '')
            AS DATE
        ) AS demog__birth_date,

        NULLIF(TRIM(person_demographics.marital_status), '')
            AS demog__marital_status,

        NULLIF(TRIM(person_demographics.yearly_income), '')
            AS demog__yearly_income,

        NULLIF(TRIM(person_demographics.gender), '')
            AS demog__gender,

        person_demographics.total_children
            AS demog__total_children,

        person_demographics.number_children_at_home
            AS demog__number_children_at_home,

        NULLIF(TRIM(person_demographics.education), '')
            AS demog__education,

        NULLIF(TRIM(person_demographics.occupation), '')
            AS demog__occupation,

        CASE person_demographics.home_owner_flag_raw
            WHEN '1' THEN TRUE
            WHEN '0' THEN FALSE
            WHEN 'true' THEN TRUE
            WHEN 'false' THEN FALSE
            ELSE NULL
        END AS demog__home_owner_flag,

        person_demographics.number_cars_owned
            AS demog__number_cars_owned,

        NULLIF(TRIM(person_demographics.commute_distance), '')
            AS demog__commute_distance,

        NULLIF(TRIM(store_demographics.contact_name), '')
            AS store_demog__contact_name,

        NULLIF(TRIM(store_demographics.job_title), '')
            AS store_demog__job_title,

        NULLIF(TRIM(store_demographics.business_type), '')
            AS store_demog__business_type,

        store_demographics.year_opened
            AS store_demog__year_opened,

        NULLIF(TRIM(store_demographics.specialty), '')
            AS store_demog__specialty,

        store_demographics.square_feet
            AS store_demog__square_feet,

        NULLIF(TRIM(store_demographics.brands), '')
            AS store_demog__brands,

        NULLIF(TRIM(store_demographics.internet), '')
            AS store_demog__internet,

        store_demographics.number_employees
            AS store_demog__number_employees,

        TRY_CAST(s.sales_person_id AS INTEGER)
            AS assigned_sales_person_id,

        GREATEST(
            TRY_CAST(c.modified_date AS TIMESTAMP),
            TRY_CAST(p.modified_date AS TIMESTAMP),
            TRY_CAST(s.modified_date AS TIMESTAMP),
            TRY_CAST(territory.modified_date AS TIMESTAMP),
            email.source_modified_at,
            phone.source_modified_at,
            address.source_modified_at
        ) AS source_modified_at

    FROM sales.customer AS c

    LEFT JOIN person.person AS p
        ON c.person_id = p.business_entity_id

    LEFT JOIN sales.store AS s
        ON c.store_id = s.business_entity_id

    LEFT JOIN sales.sales_territory AS territory
        ON c.territory_id = territory.territory_id

    LEFT JOIN email_ranked AS email
        ON c.person_id = email.business_entity_id

    LEFT JOIN phone_ranked AS phone
        ON c.person_id = phone.business_entity_id

    LEFT JOIN address_ranked AS address
        ON COALESCE(c.store_id, c.person_id) = address.business_entity_id

    LEFT JOIN person_demographics
        ON c.person_id = person_demographics.business_entity_id

    LEFT JOIN store_demographics
        ON c.store_id = store_demographics.business_entity_id
)

SELECT
    src.*,

    md5(
        CAST(
            to_json(
                struct_pack(
                    account_number := src.account_number,
                    person_id := src.person_id,
                    store_id := src.store_id,
                    customer_type := src.customer_type,
                    customer_name := src.customer_name,

                    person_type := src.person_type,
                    name_style := src.name_style,
                    title := src.title,
                    first_name := src.first_name,
                    middle_name := src.middle_name,
                    last_name := src.last_name,
                    suffix := src.suffix,
                    email_promotion := src.email_promotion,

                    email_address := src.email_address,
                    phone_number := src.phone_number,
                    phone_number_type := src.phone_number_type,

                    address_type := src.address_type,
                    address_line1 := src.address_line1,
                    address_line2 := src.address_line2,
                    city := src.city,
                    state_province_code := src.state_province_code,
                    state_province_name := src.state_province_name,
                    postal_code := src.postal_code,
                    country_region_code := src.country_region_code,
                    country_region_name := src.country_region_name,

                    territory_id := src.territory_id,
                    territory_name := src.territory_name,
                    territory_group := src.territory_group,
                    territory_country_region_code :=
                        src.territory_country_region_code,

                    demog__date_first_purchase :=
                        src.demog__date_first_purchase,
                    demog__birth_date := src.demog__birth_date,
                    demog__marital_status := src.demog__marital_status,
                    demog__yearly_income := src.demog__yearly_income,
                    demog__gender := src.demog__gender,
                    demog__total_children := src.demog__total_children,
                    demog__number_children_at_home :=
                        src.demog__number_children_at_home,
                    demog__education := src.demog__education,
                    demog__occupation := src.demog__occupation,
                    demog__home_owner_flag := src.demog__home_owner_flag,
                    demog__number_cars_owned := src.demog__number_cars_owned,
                    demog__commute_distance := src.demog__commute_distance,

                    store_demog__contact_name :=
                        src.store_demog__contact_name,
                    store_demog__job_title := src.store_demog__job_title,
                    store_demog__business_type :=
                        src.store_demog__business_type,
                    store_demog__year_opened :=
                        src.store_demog__year_opened,
                    store_demog__specialty := src.store_demog__specialty,
                    store_demog__square_feet :=
                        src.store_demog__square_feet,
                    store_demog__brands := src.store_demog__brands,
                    store_demog__internet := src.store_demog__internet,
                    store_demog__number_employees :=
                        src.store_demog__number_employees,

                    assigned_sales_person_id :=
                        src.assigned_sales_person_id
                )
            ) AS VARCHAR
        )
    ) AS attribute_hash

FROM customer_source AS src;

