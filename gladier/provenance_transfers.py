from gladier_tools.globus import Transfer


class DistCrateTransfer(Transfer):
    """
    Stub of a class responsible specifically for transferring Dist Step crates back to the orchestration server.
    """
    # TODO: have this handle the state augmentation currently in ProvenanceBaseClient.get_flow_definition()
    required_input = [
        'prov_compute_GCS_id',
        'orchestration_server_endpoint_id',  # TODO: these first two may be more dynamic?
        '_provenance_crate_destination_directory',
    ]
    alias_exempt = [
        'prov_compute_GCS_id',
        'orchestration_server_endpoint_id',
        '_provenance_crate_destination_directory',
    ]
