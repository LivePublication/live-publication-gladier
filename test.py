from gladier import GladierBaseTool, generate_flow_definition, GladierBaseClient
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
class MakeDirs(GladierBaseTool):
    """List files on the filesystem"""

    compute_functions = [makedirs]
    required = ["name"]
    flow_input = {"mode": 0o777, "exist_ok": False}


@generate_flow_definition()
class ListDirs(GladierBaseTool):
    """List files on the filesystem"""

    compute_functions = [ls]
    required = ["path"]

@generate_flow_definition()
class client(GladierBaseClient):
    gladier_tools = [
        "gladier_tools.globus.Transfer:ToCompute",
        MakeDirs,
        ListDirs,
        ]





test_tool = MakeDirs()
# print(json.dumps(test_tool.flow_definition, indent=4))

test_client = client()
print(json.dumps(test_client.flow_definition, indent=4))