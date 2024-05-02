from gladier import generate_flow_definition, ProvenanceBaseTool, ProvenanceBaseClient
import json
from pprint import pprint

def makedirs(**data):
    """Make a directory on the filesystem"""
    import os

    os.makedirs(data["name"], mode=data["mode"], exist_ok=data["exist_ok"])
    return data["name"]

def ls(**data):
    """List files on the filesystem"""
    import os
    current_directory = os.getcwd()
    
    # List the contents of the directory
    files = os.listdir(current_directory)
    return files

@generate_flow_definition()
class MakeDirs(ProvenanceBaseTool):
    """List files on the filesystem"""

    compute_functions = [makedirs]
    required = ["name"]
    flow_input = {"mode": 0o777, "exist_ok": False}


@generate_flow_definition()
class ListDirs(ProvenanceBaseTool):
    """List files on the filesystem"""

    compute_functions = [ls]
    required = ["path"]

@generate_flow_definition(
    modifiers={
        "Provenance": {
            "OrchestrationServer_UUID": "test",
            "ProvenanceDirectory": "/tmp",
        },
    }
)
class client(ProvenanceBaseClient):
    gladier_tools = [
        # MakeDirs,
        ListDirs,
        ]
    
class ProvenanceFlow(ProvenanceBaseClient):
    gladier_tools = [
        ListDirs
    ]


# test_tool = MakeDirs()
# print(json.dumps(test_tool.flow_definition, indent=4))
# flow_test = test_tool.get_flow_definition()

test_client = client()
flow_definition = test_client.get_flow_definition()

# Sync flow with Globus and get the flow_id for prov folder
test_client.sync_flow()
flow_id = test_client.flows_manager.get_flow_id()

# flow = test_client.get_flow_definition()
# print(json.dumps(flow, indent=4))
# print(json.dumps(test_client.flow_definition, indent=4))

flow = test_client.run_flow(flow_input={
    "input": {
        'compute_endpoint': '58fb6f2d-ff78-4f39-9669-38c12d01f566', 
        'prov_compute_GCS_id': '8ee44381-114a-45de-b8f8-d105a90c200d',
        'orchestration_server_endpoint_id': '4a420ad5-4113-4d7d-aba2-883f8208e897',
        '_provenance_crate_destination_directory': flow_id
        }
    },
    label='test')

# # # Track the progress
action_id = flow["action_id"]
test_client.progress(action_id)
pprint(test_client.get_status(action_id))


