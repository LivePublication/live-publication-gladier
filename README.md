# live-publication-gladier 

This repository contains a LivePublication extension built around Gladier to define and run Globus Flows workflows with explicit, provenance-aware structure. It provides workflow/flow definitions and supporting utilities that enable generation and export of step-level execution provenance. The codebase extends Gladier with provenance-focused clients/tools and workflow definitions. It includes provenance-aware flow composition, transfer steps to emit distributed crates, and example/test flows that generate flow definitions and inputs aligned to RO-Crate packaging patterns.

## Repository structure

- `gladier/`: Provenance-aware Gladier client extensions, tools, and flow utilities.
- `tests/provenance_test.py`: Example flow definition and inputs for provenance-aware execution.
- `tests/Provenance_ROCrate_Example/`: Example WEP/WEP input JSON fixtures used for provenance/RO-Crate demonstrations.
- `tests/flow/`: Additional flow scripts and fixtures.
- `docs/`: Gladier/Globus Flows documentation and examples.

## Inputs

- Globus endpoints, transfer paths, and flow input parameters (see `tests/provenance_test.py`).
- Gladier/Globus authentication tokens and config.
- Compute function inputs for steps (e.g., reverse/sort operations in the example).

## Outputs

- Flow definition JSON (WEP) and input JSON (see `tests/Provenance_ROCrate_Example/`).
- Globus Flows actions and execution records (requires external Globus services).
- Provenance crate artifacts produced by provenance transfer steps when executed in a configured environment.

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

Globus/Gladier credentials are managed via Gladier’s login manager and stored in `~/.gladier/` (and legacy `~/.gladier-secrets.cfg` if present). You will need valid Globus credentials and endpoint IDs to run flows.

## Reproducibility notes
- Full execution depends on external Globus Flows infrastructure, Globus endpoints, and credentials.
- Example endpoint IDs in `tests/provenance_test.py` are placeholders for a specific environment.
- Provenance crate generation occurs as part of flow execution and requires configured storage targets.

## How to cite
- GitHub: https://github.com/LivePublication/live-publication-gladier
- Zenodo DOI: (minted after release)

## License
Apache-2.0. See `LICENSE`.

