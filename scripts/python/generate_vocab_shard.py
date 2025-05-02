# used to generate seed vocabulary subset
# this script filters the OMOP vocabulary tables to only include concepts found in a CDM
# taken from https://github.com/OHDSI/Tutorial-Hades/blob/main/extras/FilterVocabulary.R

from pathlib import Path
from typing import cast

import duckdb

db_file: Path = Path("./data/synthea_omop_etl.duckdb").resolve()
source_schema: str = "dbt_synthea_dev_full"
target_schema: str = "vocab_shard"

conn: duckdb.DuckDBPyConnection = duckdb.connect(db_file)

vocab_tables: list[str] = [
    "concept",
    "concept_ancestor",
    "concept_relationship",
    "concept_synonym",
    "drug_strength",
]
vocab_tables_preserve: list[str] = [
    "concept_class",
    "domain",
    "relationship",
    "vocabulary",
]
non_vocab_tables: list[str] = [
    "condition_occurrence",
    "cdm_source",
    "condition_era",
    "cost",
    "death",
    "device_exposure",
    "drug_era",
    "drug_exposure",
    "measurement",
    "observation_period",
    "observation",
    "payer_plan_period",
    "person",
    "procedure_occurrence",
    "provider",
    "visit_detail",
    "visit_occurrence",
]
concept_ids: list[int] = []
conn.sql("CREATE TEMPORARY TABLE cids(concept_id INTEGER);")
duck_concept_ids = conn.table(cids)

# TODO: add unit concepts found in drug_strength table
for table in non_vocab_tables:
    print(f"Searching table {table}")
    field_tuples: list[tuple[str]] = cast(
        "list[tuple[str]]",
        conn.sql(
            f'SELECT "name" from (PRAGMA table_info({source_schema}.{table}) WHERE "name" like \'%_concept_id\';'
        ).fetchall(),
    )
    fields: list[str] = [field for (field,) in field_tuples]
    for field in fields:
        print(f"- Searching field {field}")
        sql: str = f"SELECT DISTINCT {field} FROM {source_schema}.{table};"
        id_tuples: list[tuple[int]] = cast("list[tuple[int]]", conn.sql(sql).fetchall())
        concept_ids.extend([id for (id,) in id_tuples if (id,) is not None])

# Expand with all parents
conn.sql("CREATE TEMPORARY TABLE cids(concept_id INTEGER);")
conn.sql(f"INSERT INTO cids(concept_id) VALUES ({concept_ids});")

sql = f"""SELECT DISTINCT ancestor_concept_id 
FROM {source_schema}.concept_ancestor 
INNER JOIN cids
  ON descendant_concept_id = concept_id;"""
concept_ids.extend(conn.sql(sql).fetchall())
concept_ids = list(set(concept_ids))

# Filter data to selected concept IDs
for concept_id in concept_ids:
    if concept_id[0] is not None:
        conn.sql(f"INSERT INTO cids(concept_id) VALUES ({concept_id[0]});")

# Filter vocab tables
conn.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
for table in vocab_tables:
    print(f"Filtering table {table}")
    conn.sql(f"DROP TABLE IF EXISTS {target_schema}.{table};")
    fields = conn.sql(f"PRAGMA table_info({source_schema}.{table});").fetchall()
    fields = [field[1] for field in fields if "concept_id" in field[1]]
    sql = f"CREATE TABLE {target_schema}.{table} AS SELECT * FROM {source_schema}.{table} WHERE "
    sql += " AND ".join(
        [f"{field} IN (SELECT concept_id FROM cids)" for field in fields]
    )
    conn.sql(sql)

for table in vocab_tables_preserve:
    print(f"Migrating table {table}")
    conn.sql(f"DROP TABLE IF EXISTS {target_schema}.{table};")
    sql = f"CREATE TABLE {target_schema}.{table} AS SELECT * FROM {source_schema}.{table} "
    conn.sql(sql)

conn.close()
