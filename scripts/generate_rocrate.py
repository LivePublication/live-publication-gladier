from __future__ import annotations

from datetime import date
from pathlib import Path
import shutil
import tempfile

from rocrate.model import ContextEntity, Person
from rocrate.rocrate import ROCrate

REPO_URL = "https://github.com/LivePublication/live-publication-gladier"
DOI = "https://doi.org/10.5281/zenodo.18255883"
TITLE = "live-publication-gladier: Provenance-aware Globus Flows workflows for LivePublication"
DESCRIPTION = (
    "Provenance-aware Gladier/Globus Flows workflow definitions and utilities used "
    "to generate step-level execution provenance compatible with the LivePublication "
    "provenance model and RO-Crate packaging patterns."
)
LICENSE_URL = "https://spdx.org/licenses/Apache-2.0"
VERSION = "1.0.1"


def add_file(crate: ROCrate, repo_root: Path, rel_path: str, added_paths: set[str]) -> None:
    if rel_path in added_paths:
        return
    full_path = repo_root / rel_path
    if not full_path.exists():
        raise FileNotFoundError(f"Missing expected path: {rel_path}")
    if full_path.is_dir():
        raise IsADirectoryError(f"Expected file but found directory: {rel_path}")
    crate.add_file(source=full_path, dest_path=rel_path)
    added_paths.add(rel_path)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    crate = ROCrate()

    crate.root_dataset["name"] = TITLE
    crate.root_dataset["description"] = DESCRIPTION
    crate.root_dataset["datePublished"] = date.today().isoformat()
    crate.root_dataset["license"] = LICENSE_URL
    crate.root_dataset["version"] = VERSION
    crate.root_dataset["identifier"] = DOI

    author = Person(
        crate,
        "https://orcid.org/0000-0001-8260-231X",
        properties={"name": "Augustus Ellerm"},
    )
    crate.add(author)
    crate.root_dataset["creator"] = author

    software = ContextEntity(
        crate,
        "software",
        properties={
            "@type": "SoftwareSourceCode",
            "name": TITLE,
            "description": DESCRIPTION,
            "version": VERSION,
            "license": LICENSE_URL,
            "codeRepository": REPO_URL,
            "identifier": DOI,
            "author": author,
            "programmingLanguage": "Python",
        },
    )
    crate.add(software)
    crate.root_dataset["mainEntity"] = software

    added_paths: set[str] = set()
    for rel_path in [
        "README.md",
        "README.rst",
        "LICENSE",
        ".zenodo.json",
        "codemeta.json",
        "CITATION.cff",
        "requirements.txt",
        "setup.py",
        "setup.cfg",
        "gladier/provenance_base.py",
        "gladier/provenance_client.py",
        "gladier/provenance_transfers.py",
        "gladier/utils/flow_generation.py",
        "gladier/utils/flow_modifiers.py",
        "gladier/utils/flow_traversal.py",
        "gladier/utils/tool_chain.py",
        "gladier/utils/tool_alias.py",
        "gladier/utils/name_generation.py",
        "tests/provenance_test.py",
        "tests/Provenance_ROCrate_Example/input.json",
        "tests/Provenance_ROCrate_Example/WEP.json",
        "scripts/generate_rocrate.py",
    ]:
        add_file(crate, repo_root, rel_path, added_paths)

    has_part = crate.root_dataset.setdefault("hasPart", [])
    if {"@id": "ro-crate-metadata.json"} not in has_part:
        has_part.append({"@id": "ro-crate-metadata.json"})

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        crate.write(tmp_path)
        shutil.copy(tmp_path / "ro-crate-metadata.json", repo_root / "ro-crate-metadata.json")


if __name__ == "__main__":
    main()

