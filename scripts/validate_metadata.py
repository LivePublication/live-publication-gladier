from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
VERSION = "1.0.0"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def validate_zenodo(errors: list[str]) -> None:
    data = load_json(REPO_ROOT / ".zenodo.json")
    for field in ["title", "description", "license", "version"]:
        if not data.get(field):
            errors.append(f".zenodo.json missing required field: {field}")
    if data.get("version") != VERSION:
        errors.append(f".zenodo.json version is not {VERSION}")
    creators = data.get("creators", [])
    if not creators:
        errors.append(".zenodo.json creators is empty")
    for creator in creators:
        orcid = creator.get("orcid", "")
        if not orcid:
            errors.append(".zenodo.json creator missing orcid")
        if "http" in orcid:
            errors.append(".zenodo.json orcid must be bare, not a URL")


def validate_codemeta(errors: list[str]) -> None:
    data = load_json(REPO_ROOT / "codemeta.json")
    if data.get("version") != VERSION:
        errors.append(f"codemeta.json version is not {VERSION}")


def validate_citation(errors: list[str]) -> None:
    citation_path = REPO_ROOT / "CITATION.cff"
    try:
        import yaml  # type: ignore
    except ImportError:
        errors.append(
            "PyYAML is required to parse CITATION.cff. Install with: pip install pyyaml"
        )
        return

    with citation_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)

    if not isinstance(data, dict):
        errors.append("CITATION.cff did not parse to a mapping")
        return
    if data.get("version") != VERSION:
        errors.append(f"CITATION.cff version is not {VERSION}")


def _is_relative_local_id(value: str) -> bool:
    if value.startswith(("/", "file:", "http://", "https://")):
        return False
    return True


def validate_rocrate(errors: list[str]) -> None:
    data = load_json(REPO_ROOT / "ro-crate-metadata.json")
    graph = data.get("@graph", [])
    if not isinstance(graph, list):
        errors.append("ro-crate-metadata.json missing @graph list")
        return

    ids = [item.get("@id") for item in graph if isinstance(item, dict)]
    duplicates = {item_id for item_id in ids if ids.count(item_id) > 1}
    if duplicates:
        errors.append(f"ro-crate-metadata.json contains duplicate @id values: {sorted(duplicates)}")

    root = next((item for item in graph if item.get("@id") == "./"), None)
    if not root:
        errors.append("ro-crate-metadata.json missing root dataset (@id './')")
        return

    has_part = root.get("hasPart", [])
    for entry in has_part:
        if isinstance(entry, dict):
            entry_id = entry.get("@id")
        else:
            entry_id = entry

        if not isinstance(entry_id, str):
            continue

        if not _is_relative_local_id(entry_id):
            errors.append(f"ro-crate hasPart entry is not a relative path: {entry_id}")
            continue

        if entry_id.startswith("#"):
            continue

        if not (REPO_ROOT / entry_id).exists():
            errors.append(f"ro-crate hasPart entry does not exist: {entry_id}")


def main() -> None:
    errors: list[str] = []
    validate_zenodo(errors)
    validate_codemeta(errors)
    validate_citation(errors)
    validate_rocrate(errors)

    if errors:
        print("Metadata validation failed:")
        for error in errors:
            print(f"- {error}")
        sys.exit(1)

    print("Metadata validation passed.")
    print("\nHuman checklist:")
    print("- Confirm Quickstart steps work in your environment (or document TODOs).")
    print("- Confirm LICENSE matches Apache-2.0 intent.")
    print("- Confirm Zenodo GitHub integration is enabled.")
    print("- Create GitHub release/tag v1.0.0.")
    print("- Verify Zenodo minted DOI and update metadata to include it.")


if __name__ == "__main__":
    main()

