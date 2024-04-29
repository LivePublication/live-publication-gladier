import copy
from typing import List, Mapping, Any
from gladier.utils.tool_alias import ToolAlias
from gladier.utils.flow_traversal import iter_flow
from gladier.base import GladierBaseTool
import json

class ProvenanceBaseTool(GladierBaseTool):
  def __init__(self, alias: str = None, alias_class: ToolAlias = None):
    super().__init__(alias, alias_class)
    # Add any additional initialization code here

  def get_flow_definition(self) -> Mapping[str, Any]:
    flow_definition = super().get_flow_definition()
    for state_name, state_data in iter_flow(flow_definition):
      # Add any modifications to the flow definition here
      pass
    return flow_definition





