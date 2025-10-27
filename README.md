# Gladier with Provenance Extensions

A provenance-aware extension of [Gladier](https://gladier.readthedocs.io). This repository enhances Gladier with automatic capture and aggregation of distributed, step-level provenance metadata across workflow executions.

## Project Overview

This repository extends the upstream Gladier Python SDK to support provenance-aware workflows. The goal is to allow researchers to define and run Globus Flows using the standard Gladier API ("normative usage") while collecting provenance fragments from each distributed execution step.

When a workflow runs across multiple remote compute endpoints, each step produces a provenance fragment (formatted as an [RO-Crate](https://www.researchobject.org/ro-crate/)). These fragments are automatically transferred back to a central collection point, enabling reconstruction of a complete provenance record spanning the entire distributed computation.

## Features

This repository introduces the following provenance-specific extensions:

- **`ProvenanceBaseClient`** (`gladier/provenance_client.py`)A specialized subclass of `GladierBaseClient` that automatically injects Globus Transfer steps after each compute function to retrieve provenance crates from remote execution nodes. This client handles dynamic flow modification at runtime to construct transfer paths based on task IDs.
- **`ProvenanceBaseTool`** (`gladier/provenance_base.py`)A subclass of `GladierBaseTool` intended for tools that produce provenance. Currently scaffolding for future tool-level provenance behaviors (e.g., custom crate generation logic per tool).
- **Automatic Provenance Transfer Injection**When using `ProvenanceBaseClient`, for each compute function in your tool list, the framework automatically adds a `Transfer` step (aliased as `_provenance_{function_name}`) that:

  - Extracts the `task_id` from the compute step's output
  - Transfers the corresponding `.crate` directory from the remote node to a central orchestration server
  - Preserves the directory structure for later aggregation
- **Dynamic Path Configuration**Uses Globus Flows expression evaluation (`.=` JSONPath syntax) to dynamically construct source/destination paths at runtime based on each step's unique `task_id`.
- **Provenance Modifier Support** (`gladier/utils/flow_modifiers.py`)Defines provenance-specific flow modifiers (`"Provenance"`, `"OrchestrationServer_UUID"`, `"ProvenanceDirectory"`) for customizing provenance transfer behavior. *(Note: Modifier application logic is currently incomplete - see Limitations/TODOs below.)*

## Quickstart / Example

Below is a minimal example showing how to define and run a provenance-aware workflow:

```python
from gladier import generate_flow_definition, ProvenanceBaseClient, ProvenanceBaseTool

# Define a simple compute function
def my_analysis(**data):
    """Perform some analysis"""
    import os
    result = {"files": os.listdir(data.get("input_dir", "."))}
    return result

# Define a tool using ProvenanceBaseTool
@generate_flow_definition
class MyAnalysisTool(ProvenanceBaseTool):
    compute_functions = [my_analysis]
    required = ["input_dir"]

# Define a client using ProvenanceBaseClient
@generate_flow_definition
class MyProvenanceClient(ProvenanceBaseClient):
    gladier_tools = [
        "gladier_tools.globus.Transfer:ToCompute",  # Optional: transfer input data
        MyAnalysisTool,                              # Your compute step
        "gladier_tools.globus.Transfer:FromCompute"  # Optional: transfer results back
    ]

# Instantiate and run
client = MyProvenanceClient()
client.sync_flow()  # Register flow with Globus

flow_run = client.run_flow(flow_input={
    "input": {
        # Compute configuration
        "compute_endpoint": "<your-compute-endpoint-uuid>",
  
        # Provenance configuration
        "prov_compute_GCS_id": "<globus-collection-id-on-compute-node>",
        "orchestration_server_endpoint_id": "<central-collection-uuid>",
        "_provenance_crate_destination_directory": "<flow-id-or-custom-dir>",
  
        # Tool-specific inputs
        "input_dir": "/data/inputs",
  
        # Optional: transfer configurations
        # "to_compute_transfer_source_endpoint_id": "...",
        # ...
    }
})

# Track progress
action_id = flow_run["action_id"]
client.progress(action_id)
status = client.get_status(action_id)
```

### Required Configuration Parameters

When running a provenance-aware flow, you must provide:

- **`compute_endpoint`**: UUID of the Globus Compute endpoint where tasks will execute
- **`prov_compute_GCS_id`**: Globus Collection (formerly Endpoint) UUID on the compute node where `.crate` directories are generated
- **`orchestration_server_endpoint_id`**: Globus Collection UUID where all provenance crates will be aggregated
- **`_provenance_crate_destination_directory`**: Directory path on the orchestration server where crates will be stored (commonly set to the flow ID)

Additional parameters depend on your specific tools (e.g., transfer source/destination paths).

## TODOs

### Known Limitations

1. **Incomplete Modifier Implementation**The `apply_modifier()` function for provenance modifiers (`gladier/utils/flow_modifiers.py`) is incomplete. The modifiers `"OrchestrationServer_UUID"` and `"ProvenanceDirectory"`  are planned for implementation.
2. **Multiple Compute Functions Per Tool** Support aggregation of multuple provenance fragements from the *same* node.
3. **Hardcoded Assumptions**

   - Crate naming convention: `{task_id}.crate`
   - Crate location: Assumes crates are accessible at the root of the compute node's Globus Collection
   - Transfer parameters: Uses `recursive=True` and specific JSONPath expressions

## Installation

**Note**: This is a research prototype. Installation instructions assume local development.

```bash
git clone https://github.com/LivePublication/live-publication-gladier.git
cd live-publication-gladier
pip install -e .
```

Dependencies are listed in `requirements.txt` and include:

- `globus-compute-sdk`
- `globus-automate-client`
- `globus-sdk`
- Other Globus and utility libraries

## Acknowledgement

This project builds upon the Gladier framework developed by the Globus team. See the [upstream Gladier repository](https://github.com/globus-gladier/gladier) for the original project.
