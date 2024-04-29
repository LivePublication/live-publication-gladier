from gladier import generate_flow_definition, ProvenanceBaseTool, ProvenanceBaseClient
import json

def makedirs(**data):
    """Make a directory on the filesystem"""
    import os

    os.makedirs(data["name"], mode=data["mode"], exist_ok=data["exist_ok"])
    return data["name"]

def ls(**data):
    """List files on the filesystem"""
    import os

    return os.listdir(data["path"])

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
        MakeDirs,
        ListDirs,
        ]


# test_tool = MakeDirs()
# print(json.dumps(test_tool.flow_definition, indent=4))
# flow_test = test_tool.get_flow_definition()

test_client = client()
flow = test_client.get_flow_definition()
# print(json.dumps(flow, indent=4))
# print(json.dumps(test_client.flow_definition, indent=4))

test_client.run_flow(flow_input={
    "input": {
        'compute_endpoint': '58fb6f2d-ff78-4f39-9669-38c12d01f566', 
        'prov_compute_GCS_id': '8ee44381-114a-45de-b8f8-d105a90c200d',
        'orchestration_server_endpoint_id': '4a420ad5-4113-4d7d-aba2-883f8208e897',
        
        '_provenance_makedirs_transfer_destination_path': 'crates/makedirs/',
        '_provenance_ls_transfer_destination_path': 'crates/ls/'
        }
    },
    label='test')



