import json
from pathlib import Path

from gladier import ProvenanceBaseClient, generate_flow_definition, ProvenanceBaseTool

# Flow configuration details
exp_compute_endpoint_uuid = '58fb6f2d-ff78-4f39-9669-38c12d01f566'
exp_compute_GCS_uuid = '8ee44381-114a-45de-b8f8-d105a90c200d'
o_server_GCS_uuid = '4a420ad5-4113-4d7d-aba2-883f8208e897'
data_store_uuid = 'b782400e-3e59-412c-8f73-56cd0782301f'

ToCompute_source_uuid = data_store_uuid
ToCompute_dest_uuid = exp_compute_GCS_uuid
ToCompute_source_path = '/input/test.txt'  # step in = run in
ToCompute_dest_path = '/input/test.txt'
ToCompute_recursive = False

FromCompute_source_uuid = exp_compute_GCS_uuid
FromCompute_dest_uuid = data_store_uuid
FromCompute_source_path = '/output/test.txt'
FromCompute_dest_path = '/output/test.txt'  # step out = run out
FromCompute_recursive = False


def rev_txt(input_file, output_file):
    with open(input_file, 'r') as f:
        lines = f.readlines()
    lines = [line.strip() for line in lines]
    lines = [line[::-1] for line in lines if line]
    with open(output_file, 'w') as f:
        f.write('\n'.join(lines))


@generate_flow_definition()
class ReverseText(ProvenanceBaseTool):
    """Reverse each line in a text file"""

    compute_functions = [rev_txt]


def sort_txt(input_file: str, output_file: str, reverse: bool):
    with open(input_file, 'r') as f:
        lines = f.readlines()
    lines = [line.strip() for line in lines]
    lines = [line for line in lines if line]
    lines.sort()
    if reverse:
        lines = lines[::-1]
    with open(output_file, 'w') as f:
        f.write('\n'.join(lines))


@generate_flow_definition()
class SortText(ProvenanceBaseTool):
    """Sort the lines in a text file"""

    compute_functions = [sort_txt]


@generate_flow_definition
class client(ProvenanceBaseClient):
    """Workflow matching the provenance rocrate example"""
    gladier_tools = [
        "gladier_tools.globus.Transfer:ToCompute",
        ReverseText,
        # TODO - transfer between steps?
        SortText,
        "gladier_tools.globus.Transfer:FromCompute",
        ]

    globus_group = "9e155e5c-e011-11ee-a4a3-8fbdadf65a0b"


if __name__ == '__main__':
    folder = Path(__file__).parent / 'Provenance_ROCrate_Example'
    folder.mkdir(exist_ok=True)

    test_client = client()
    test_client.flows_manager.flow_title = 'Provenance ROCrate Example'

    # Write WEP.json
    flow_definition = test_client.get_flow_definition()
    output_file = folder / 'WEP.json'
    with open(output_file, 'w') as f:
        json.dump(flow_definition, f, indent=4)

    # Register flow
    test_client.flows_manager.register_flow()
    test_client.sync_flow()
    flow_id = test_client.flows_manager.get_flow_id()

    # Define input
    _input = {
        'input': {
            # Compute & provenance config
            'compute_endpoint': exp_compute_endpoint_uuid,
            'prov_compute_GCS_id': exp_compute_GCS_uuid,
            'orchestration_server_endpoint_id': o_server_GCS_uuid,
            '_provenance_crate_destination_directory': flow_id,

            # Transfer configs
            "to_compute_transfer_source_endpoint_id": ToCompute_source_uuid,
            "to_compute_transfer_destination_endpoint_id": ToCompute_dest_uuid,
            "to_compute_transfer_source_path": ToCompute_source_path,
            "to_compute_transfer_destination_path": ToCompute_dest_path,
            "to_compute_transfer_recursive": ToCompute_recursive,

            "from_compute_transfer_source_endpoint_id": FromCompute_source_uuid,
            "from_compute_transfer_destination_endpoint_id": FromCompute_dest_uuid,
            "from_compute_transfer_source_path": FromCompute_source_path,
            "from_compute_transfer_destination_path": FromCompute_dest_path,
            "from_compute_transfer_recursive": FromCompute_recursive,

            # Compute inputs
            'RevTxt': {   # Note that these are name from the function, not the tool, as tools may have multiple functions
                'input_file': '/input/test.txt',
                'output_file': '/output/test.txt',
            },
            'SortTxt': {
                'input_file': '/input/test.txt',
                'output_file': '/output/test.txt',
                'reverse': True,
            },
        }}

    # Check required inputs of tools are present in input
    # TODO: do something more with this information
    def _chain_get(data: dict, key: str):
        keys = key.split('.')
        for k in keys:
            data = data[k]
        return data

    for tool in test_client.tools:
        if len(tool.compute_functions) > 0:
            required = tool.get_required_input()
            for req in required:
                try:
                    val = _chain_get(_input['input'], req)
                    print(f"Required input {req} found in input with value {val}")
                except KeyError:
                    print(f"Required input {req} not found in input")

    # Combine default inputs (e.g.: function ids) with explicit inputs and save to file
    full_input = test_client.get_input()
    full_input['input'].update(_input['input'])
    output_file = folder / 'input.json'
    with open(output_file, 'w') as f:
        json.dump(full_input, f, indent=4)

    # TODO: run flow
    for tool in test_client.tools:
        test_client.check_input(tool, full_input)
