import json
from pathlib import Path
from pprint import pprint

from BeeMovieScript import GetBeeMovieScript
from gladier import generate_flow_definition, ProvenanceBaseClient
from ls import ListDirs

# Flow configuration details
exp_compute_endpoint_uuid = 'e8c8b86f-a6fb-4c5b-9bc0-45423336f0c5'
exp_compute_GCS_uuid = 'abe41251-eb3c-4cc5-b329-cf9fe8fadcf1'
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


@generate_flow_definition
class client(ProvenanceBaseClient):
    gladier_tools = [
        "gladier_tools.globus.Transfer:ToCompute",
        ListDirs,
        GetBeeMovieScript,
        "gladier_tools.globus.Transfer:FromCompute",
        ]
    
    globus_group = "9e155e5c-e011-11ee-a4a3-8fbdadf65a0b"


if __name__ == '__main__':
    test_client = client()
    test_client.flows_manager.flow_title = 'Test flow'

    # Dump WEP to file
    flow_definition = test_client.get_flow_definition()
    output_dir = Path(__file__).parent / 'Example_Collection'
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / 'WEP.json'

    with open(output_file, 'w') as f:
        json.dump(flow_definition, f, indent=4)

    print(f"Flow definition saved to {output_file}")

    # Sync flow with Globus and get the flow_id for prov folder
    test_client.flows_manager.register_flow()
    test_client.sync_flow()
    flow_id = test_client.flows_manager.get_flow_id()

    _input = {
        "input": {
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

            'Ls': {
                'data': {}
            },
            'Getscript': {
                'data': {}
            }
        }}

    # Combine default inputs (e.g.: function ids) with explicit inputs and save to file
    full_input = test_client.get_input()
    full_input['input'].update(_input['input'])
    output_file = output_dir / 'input.json'
    with open(output_file, 'w') as f:
        json.dump(full_input, f, indent=4)

    # Run flow and track progress
    flow = test_client.run_flow(flow_input=_input, label='test flow')

    action_id = flow["action_id"]
    test_client.progress(action_id)
    pprint(test_client.get_status(action_id))
