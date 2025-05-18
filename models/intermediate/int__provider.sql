SELECT
    row_number() OVER (ORDER BY provider_state, provider_city, provider_zip, provider_id) AS provider_id
    , rowid AS provider_id_2
    , provider_name
    , {{ dbt.cast("null", api.Column.translate_type("varchar(20)")) }} AS npi
    , {{ dbt.cast("null", api.Column.translate_type("varchar(20)")) }} AS dea
    , 38004446 AS specialty_concept_id
    , {{ dbt.cast("null", api.Column.translate_type("integer")) }} AS care_site_id
    , {{ dbt.cast("null", api.Column.translate_type("integer")) }} AS year_of_birth
    , CASE upper(provider_gender)
        WHEN 'M' THEN 8507
        WHEN 'F' THEN 8532
    END AS gender_concept_id
    , provider_id AS provider_source_value
    , provider_specialty AS specialty_source_value
    , 38004446 AS specialty_source_concept_id
    , provider_gender AS gender_source_value
    , CASE upper(provider_gender)
        WHEN 'M' THEN 8507
        WHEN 'F' THEN 8532
    END AS gender_source_concept_id
FROM {{ ref( 'stg_synthea__providers') }}
