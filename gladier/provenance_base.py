import inspect
from typing import List, Mapping, Any

from gladier.base import GladierBaseTool
from gladier.utils.flow_traversal import iter_flow
from gladier.utils.name_generation import get_compute_flow_state_name
from gladier.utils.tool_alias import ToolAlias


class ProvenanceBaseTool(GladierBaseTool):
    def __init__(self, alias: str = None, alias_class: ToolAlias = None):
        super().__init__(alias, alias_class)
        # Add any additional initialization code here

    def get_flow_definition(self) -> Mapping[str, Any]:
        flow_definition = super().get_flow_definition()
        for state_name, state_data in iter_flow(flow_definition):
            # Add any modifications to the flow definition here
            # TODO: Add function parameters as parameters to flow definition?
            pass
        return flow_definition

    def get_function_inputs(self) -> Mapping[str, type]:
        """
        Get the input parameters for each compute function in the tool.
        :return: A dict of function parameter names to their types
        """
        inputs = {}
        for func in self.compute_functions:
            fname = get_compute_flow_state_name(func)
            sig = inspect.signature(func)
            for param in sig.parameters.values():
                inputs[f'{fname}.{param.name}'] = param.annotation

        return inputs

    def get_required_input(self) -> List[str]:
        required = super().get_required_input()

        # Add compute function parameters as required inputs
        # TODO: check if this conflicts with intended usage of required_inputs
        required.extend(self.get_function_inputs().keys())

        return required
