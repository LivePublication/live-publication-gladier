import logging

from gladier.exc import FlowModifierException
from gladier.utils.name_generation import get_compute_flow_state_name
import json

log = logging.getLogger(__name__)
compute_modifiers = {"endpoint", "payload", "tasks"}
# All top level states can be modified.
# https://globus-automate-client.readthedocs.io/en/latest/authoring_flows.html#action-state-type
state_modifiers = {
    "Type",
    "ActionUrl",
    "WaitTime",
    "ExceptionOnActionFailure",
    "RunAs",
    "InputPath",
    "Parameters",
    "ResultPath",
    "Catch",
    "ActionScope",
    "Next",
    "End",
}

provenance_modifiers = {
    "Provenance",
    "OrchestrationServer_UUID",
    "ProvenanceDirectory"
}


class FlowModifiers:
    supported_modifiers = state_modifiers.union(compute_modifiers, provenance_modifiers)
    compute_modifiers = compute_modifiers
    state_modifiers = state_modifiers
    provenance_modifiers = provenance_modifiers

    def __init__(self, tools, modifiers, cls=None):
        self.cls = cls
        self.tools = tools
        self.functions = [
            func for tool in tools for func in getattr(tool, "compute_functions", [])
        ]
        self.function_names = [f.__name__ for f in self.functions]
        self.state_names = [get_compute_flow_state_name(f) for f in self.functions]
        self.modifiers = modifiers
        self.check_modifiers()

    def get_function(self, function_identifier):
        if function_identifier in self.function_names:
            return self.functions[self.function_names.index(function_identifier)]
        if function_identifier in self.functions:
            return function_identifier

    def get_flow_state_name(self, function_identifier):
        func = self.get_function(function_identifier)
        return get_compute_flow_state_name(func)

    def get_state_result_path(self, state_name):
        return f"$.{state_name}.details.results"

    def check_modifiers(self):
        log.debug(f"Checking modifiers: {self.modifiers}")
        if not isinstance(self.modifiers, dict):
            raise FlowModifierException(f"{self.cls}: Flow Modifiers must be a dict")

        legacy_funcs = [
            func for tool in self.tools for func in getattr(tool, "funcx_functions", [])
        ]
        legacy_func_names = [f.__name__ for f in legacy_funcs]

        # Check if modifiers were set correctly
        for name, mods in self.modifiers.items():
            if name in legacy_func_names:
                raise FlowModifierException(
                    f"Class {self.cls} is a Legacy Gladier tool pre-v0.9.0. Please use a modern "
                    "version or follow the migration guide here to make it compatible: \n\n"
                    "\thttps://gladier.readthedocs.io/en/latest/migration.html\n"
                )

            if not self.get_function(name) and name not in provenance_modifiers:
                print(self.cls.__class__.__name__)
                raise FlowModifierException(
                    f"Class {self.cls} included modifier which does not "
                    f"exist: {name}. Allowed modifiers include "
                    f'{", ".join(self.function_names)} and'
                    f" {", ".join(provenance_modifiers)}"
                )

            for mod_name, mod_value in mods.items():
                if mod_name not in self.supported_modifiers:
                    raise FlowModifierException(
                        f"Class {self.cls}: Unsupported modifier "
                        f'"{mod_name}". The only supported modifiers are: '
                        f"{self.supported_modifiers}"
                    )

    def apply_modifiers(self, flow):
        for name, mods in self.modifiers.items():
            if name in provenance_modifiers:
                flow_states = list(flow["States"].items())
                for index, (state_name, state) in enumerate(flow_states):
                    if ("provenance" in state_name
                            and state.get("ActionUrl") == "https://actions.automate.globus.org/transfer/transfer"):
                        log.debug(f"Configuring provenance transfer {state_name}" +
                                  f"for provenance compute step {flow_states[index - 1][0]}")
                        flow["States"][state_name] = self.apply_modifier(
                            flow["States"][state_name], mods, flow_states[index - 1]
                        )
            else:
                state_name = self.get_flow_state_name(name)
                flow["States"][state_name] = self.apply_modifier(
                    flow["States"][state_name], mods
                )

        return flow

    def apply_modifier(self, flow_state, state_modifiers, prev_flow_state=None):
        if prev_flow_state:
            # only included if provenance modifiers are present
            try:
                # TODO: Currently doesnt do anything.
                OS_UUID = state_modifiers["OrchestrationServer_UUID"]
                Prov_DIR = state_modifiers["ProvenanceDirectory"]
                prev_state_name, prev_state = prev_flow_state

                return flow_state
            except KeyError as e:
                print("Failed to resolve OS_UUID or Prov_DIR:", str(e))
        else:
            for modifier_name, value in state_modifiers.items():
                log.debug(f'Applying modifier "{modifier_name}", value "{value}"')
                # If this is for a compute task
                if modifier_name in self.compute_modifiers:
                    if modifier_name == "tasks":
                        flow_state["Parameters"] = self.generic_set_modifier(
                            flow_state["Parameters"], "tasks", value
                        )
                    else:
                        flow_state["Parameters"]["tasks"] = [
                            self.generic_set_modifier(fx_task, modifier_name, value)
                            for fx_task in flow_state["Parameters"]["tasks"]
                        ]
                elif modifier_name in self.state_modifiers:
                    self.generic_set_modifier(flow_state, modifier_name, value)
            return flow_state

    def generic_set_modifier(self, item, mod_name, mod_value):
        if not isinstance(mod_value, str):
            if mod_value in self.functions:
                sn = get_compute_flow_state_name(mod_value)
                mod_value = self.get_state_result_path(sn)
        elif isinstance(mod_value, str) and not mod_value.startswith("$."):
            if mod_value in self.function_names:
                sn = self.state_names[self.function_names.index(mod_value)]
                mod_value = self.get_state_result_path(sn)
            elif mod_value in self.state_names:
                mod_value = self.get_state_result_path(mod_value)
            elif mod_name not in state_modifiers:
                mod_value = f"$.input.{mod_value}"

        # Remove duplicate keys
        for duplicate_mod_key in (mod_name, f"{mod_name}.$"):
            if duplicate_mod_key in item.keys():
                item.pop(duplicate_mod_key)

        # Note: Top level State types don't end with '.$', all others must end with
        # '.$' to indicate the value should be replaced. '.=' is not supported or possible yet
        if isinstance(mod_value, str) and mod_value.startswith("$."):
            mod_name = f"{mod_name}.$"
        item[mod_name] = mod_value
        log.debug(f"Set modifier {mod_name} to {mod_value}")
        return item
