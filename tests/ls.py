from gladier import ProvenanceBaseTool, generate_flow_definition

def ls(**data):
    """List files on the filesystem"""
    import os
    current_directory = os.getcwd()
    
    # List the contents of the directory
    files = os.listdir(current_directory)
    return files

@generate_flow_definition()
class ListDirs(ProvenanceBaseTool):
    """List files on the filesystem"""

    compute_functions = [ls]
    required = ["path"]