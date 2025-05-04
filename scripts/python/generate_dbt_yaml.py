# A script to generate dbt YAML files from the OMOP CDM documentation
#
# Requires `BeautifulSoup4` and `ruamel.yaml` to be installed
# Get the OMOP CDM documentation with e.g.:
#   `wget https://raw.githubusercontent.com/OHDSI/CommonDataModel/refs/heads/main/docs/cdm54.html`

import argparse
from dataclasses import dataclass
from pathlib import Path

from bs4 import BeautifulSoup
from bs4._typing import _AtMostOneElement
from bs4.element import Tag
from ruamel.yaml import YAML


@dataclass
class omop_documentation_container:
    cdm_field: str
    user_guide: str
    etl_conventions: str
    datatype: str
    required: bool
    primary_key: bool
    foreign_key: bool
    foreign_key_table: str
    foreign_key_domain: str


@dataclass
class CliArgs:
    """A dataclass to ensure correct typing of command line arguments"""

    cdm_html: Path = Path()
    output_dir: Path = Path()


def parse_cli_arguments() -> CliArgs:
    """
    Parse command line arguments.

    Returns:
         CLIArgs DataClass containing cdm_html: Path() and output_dir: Path().
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="""
        Generate dbt YAML files from the OMOP CDM documentation.
        For example: python generate_dbt_yaml.py cdm54.html ./output
        """
    )

    # Common data model url.
    _ = parser.add_argument(
        "cdm_html", type=Path, help="Path to the OMOP CDM documentation HTML"
    )

    # Output Directory.
    _ = parser.add_argument(
        "output_dir", type=Path, help="Path to the output directory"
    )

    # Store paths in CLIArgs data class.
    args: CliArgs = parser.parse_args(namespace=CliArgs())

    if not args.cdm_html.exists():
        parser.exit(1, f"File {args.cdm_html} does not exist")
    if not args.output_dir.exists():
        parser.exit(1, f"Directory {args.output_dir} does not exist")

    return args


def table_handler(table: Tag) -> list[OmopDocumentationContainer]:
    """
    Takes a table and returns a list of objects that represent the tables in the table.
    """
    rows: list[Tag] = [_ensure_tag(div) for div in table.find_all("tr")]

    return [row_handler(row) for row in rows]


def row_handler(row: Tag) -> OmopDocumentationContainer:
    """
    Take each row from a table and handle it, resulting in a object that can neatly
    store how the CDM docs express each column.
    """
    cell_tags: list[Tag] = [_ensure_tag(cell) for cell in row.find_all("td")]

    cells_raw: dict[str, str] = {
        "cdm_field": cell_tags[0].get_text(),
        "user_guide": cell_tags[1].get_text(),
        "etl_conventions": cell_tags[2].get_text(),
        "datatype": cell_tags[3].get_text(),
        "required": cell_tags[4].get_text(),
        "primary_key": cell_tags[5].get_text(),
        "foreign_key": cell_tags[6].get_text(),
        "foreign_key_table": cell_tags[7].get_text(),
        "foreign_key_domain": cell_tags[8].get_text(),
    }

    # Remove dangling whitespace and newlines from parsed HTML
    cells_stripped: dict[str, str] = {
        k: v.replace("\n", "").strip() for k, v in cells_raw.items()
    }

    # Convert sentinels to booleans. Assign values to omop_documentation_container DataClass.
    return OmopDocumentationContainer(
        cdm_field=cells_stripped["cdm_field"],
        user_guide=cells_stripped["user_guide"],
        etl_conventions=cells_stripped["etl_conventions"],
        datatype=cells_stripped["datatype"],
        required=sentinel_to_bool(cells_stripped["required"]),
        primary_key=sentinel_to_bool(cells_stripped["primary_key"]),
        foreign_key=sentinel_to_bool(cells_stripped["foreign_key"]),
        foreign_key_table=cells_stripped["foreign_key_table"],
        foreign_key_domain=cells_stripped["foreign_key_domain"],
    )


def sentinel_to_bool(text: str) -> bool:
    if text == "Yes":
        return True
    else:
        return False


def extract_table_description(table_handle: Tag) -> str:
    sibling_element: _AtMostOneElement = _ensure_tag(
        table_handle.find("p", string="Table Description")
    ).next_sibling
    description_element: _AtMostOneElement = _ensure_tag(sibling_element).next_sibling
    description: str = _ensure_tag(description_element).get_text()

    return description.replace("\n", " ")


def omop_docs_to_dbt_config(obj: omop_documentation_container) -> dict[str, str]:


def omop_docs_to_dbt_config(obj: OmopDocumentationContainer) -> dict[str, str]:
    """
    With an OMOP documentation object, we can use some simple string parsing/heuristic
    to create dbt test configs.
    """
    column_config = {
        "name": obj.cdm_field,
        "description": obj.user_guide,
        "data_type": obj.datatype,
    }

    # == Create Tests ==
    tests: list = []

    if obj.required:
        tests.append("not_null")

    if obj.primary_key:
        tests.append("unique")

    if obj.foreign_key:
        if obj.foreign_key_domain == "":
            # Handle simpler cases first, where a domain is not constrained
            test = {
                "relationships": {
                    "to": f"ref('{obj.foreign_key_table.lower()}')",
                    "field": f"{obj.foreign_key_table.lower()}_id",
                }
            }
            tests.append(test)

        else:
            # Add constrained domain tests
            specific_test = {
                "dbt_utils.relationships_where": {
                    "to": f"ref('{obj.foreign_key_table.lower()}')",
                    "field": f"{obj.foreign_key_table.lower()}_id",
                    "from_condition": f"{obj.cdm_field} <> 0",
                    "to_condition": f"domain_id = '{obj.foreign_key_domain}'",
                }
            }
            tests.append(specific_test)

    if tests:
        column_config["tests"] = tests

    return column_config


def extract_omop_table_names(soup_obj: BeautifulSoup) -> list[str]:
    """
    Dynamically extract table names from the OMOP CDM documentation
    """
    headers: list[Tag] = [
        _ensure_tag(div)
        for div in soup_obj.find_all(
            "div", attrs={"class": "section level3 tabset tabset-pills"}
        )
    ]

    table_names: list[str] = []
    for div in headers:
        table_name_element: Tag = _ensure_tag(div.find("h3"))
        table_names.append((table_name_element.get_text()))

    # Exclude unwanted tables
    unwanted_tables: set[str] = {"cohort", "cohort_definition"}
    filtered_table_names: list[str] = [
        table for table in table_names if table not in unwanted_tables
    ]
    return filtered_table_names


def _ensure_tag(element: _AtMostOneElement) -> Tag:
    """
    Ensures that the given element is a BeautifulSoup Tag.
    If the element is not a Tag, raises a ValueError.
    """
    if isinstance(element, Tag):
        return element
    raise ValueError("No Tag returned from BeautifulSoup query")


def create_table_dict(
    table: str,
    table_description: str,
    parsed_table: list[OmopDocumentationContainer],
):
    pass

    # table_dict structure - dict[str, list[dict[str, str | list[dict[str, str]]]]]
    #  {
    #     "models": [
    #         {
    #             "name": table,
    #             "description": table_description,
    #             "columns": [omop_docs_to_dbt_config(doc_container) for doc_container in parsed_table],
    #         }
    #     ]
    # }


def main(
    cdm_docs_path: Path,
    output_dir: Path,
) -> None:
    """
    Main loop to generate dbt YAML files from the OMOP CDM documentation
    """
    with open(cdm_docs_path) as file_handle:
        file: str = file_handle.read()

    soup: BeautifulSoup = BeautifulSoup(file, features="html.parser")

    tables: list[str] = extract_omop_table_names(soup)
    print(f" Found {len(tables)} tables in the OMOP CDM documentation")

    for table in tables:
        # For each table generate the desired dbt yaml
        # Get desired div with table
        div_handle: Tag = _ensure_tag(soup.find("div", attrs={"id": table}))
        table_handle: Tag = _ensure_tag(div_handle.find("table"))
        tbody_handle: Tag = _ensure_tag(table_handle.find("tbody"))

        parsed_table: list[OmopDocumentationContainer] = table_handler(tbody_handle)
        table_description: str = extract_table_description(table_handle)

        table_dict = create_table_dict(table, table_description, parsed_table)

        table_dict = {
            "models": [
                {
                    "name": table,
                    "description": table_description,
                    "columns": [omop_docs_to_dbt_config(obj) for obj in parsed_table],
                }
            ]
        }

        yaml: YAML = YAML()
        yaml.indent(mapping=2, sequence=4, offset=2)  # pyright: ignore[reportUnknownMemberType]
        yaml.width = 100
        yaml.dump(table_dict, open(f"{output_dir}/{table}.yml", "w"))  # pyright: ignore[reportUnknownMemberType]
        yaml.allow_duplicate_keys

    print(f" Exported to `{output_dir}`")
    print("  Done!")


if __name__ == "__main__":
    args: CliArgs = parse_cli_arguments()
    main(args.cdm_html, args.output_dir)
