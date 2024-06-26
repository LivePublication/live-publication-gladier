from __future__ import annotations

import typing as t
import types

import logging
import os
import pathlib
from collections.abc import Iterable

import gladier
import gladier.exc
import gladier.managers.compute_login_manager
import gladier.storage.config
import gladier.storage.migrations
import gladier.utils.automate
import gladier.utils.dynamic_imports
import gladier.utils.name_generation
import gladier.utils.tool_alias
import gladier.version
from gladier.base import GladierBaseTool
from gladier.client import GladierBaseClient
from gladier.managers import ComputeManager, FlowsManager
from gladier.managers.login_manager import (
    AutoLoginManager,
    BaseLoginManager,
    ConfidentialClientLoginManager,
)
from gladier.storage.tokens import GladierSecretsConfig
import pprint

log = logging.getLogger(__name__)

class ProvenanceBaseClient(GladierBaseClient):
    """
    ProvenanceBaseClient is a specialized subclass of GladierBaseClient designed to handle
    the provenance of data in automated workflows. It can be extended with specific tools
    and functionalities that cater to the requirements of data provenance in a distributed
    environment.
    """

    def __init__(
        self,
        auto_registration: bool = True,
        login_manager: t.Optional[BaseLoginManager] = None,
        flows_manager: t.Optional[FlowsManager] = None,
    ):
        self._tools = None
        self.storage = self._determine_storage()
        self.login_manager = login_manager or self._determine_login_manager(
            self.storage
        )

        self.flows_manager = flows_manager or FlowsManager(
            auto_registration=auto_registration, subscription_id=self.subscription_id
        )
        if self.globus_group:
            self.flows_manager.globus_group = self.globus_group
        if not self.flows_manager.flow_title:
            self.flows_manager.flow_title = f"{self.__class__.__name__} flow"

        self.compute_manager = ComputeManager(auto_registration=auto_registration)
        self.storage.update()

        for man in (self.flows_manager, self.compute_manager):
            man.set_storage(self.storage, replace=False)
            man.set_login_manager(self.login_manager, replace=False)
            man.register_scopes()

    @property
    def tools(self):
        """
        Override the GladierBaseClient tools property to automatically
        include provenance transfer steps for each provenance compute function
        """
        if getattr(self, "_tools", None):
            return self._tools

        gtools = getattr(self, "gladier_tools", [])
        if not gtools or not isinstance(gtools, Iterable):
            if not self.get_flow_definition():
                raise gladier.exc.ConfigException(
                    '"gladier_tools" must be a defined list of Gladier Tools. '
                    'Ex: ["gladier.tools.hello_world.HelloWorld"]'
                )
        
        for gt in gtools:
            if isinstance(gt, types.FunctionType):
                for count, func in enumerate(gt.compute_functions):
                    log.debug(f"Adding provenance transfer step for {func.__name__}")
                    """ 
                    TODO: How do multiple compute functions executed as a 
                    single tool/step get handled? Dist Step Crate per function?
                    """
                    gtools.insert(gtools.index(gt) + count + 1, f"gladier_tools.globus.Transfer:_provenance_{func.__name__}")

        self._tools = [
            self.get_gladier_defaults_cls(gt, self.alias_class) for gt in gtools
        ]

        return self._tools
    
    def get_flow_definition(self):
        """
        Get the flow definition attached to this class. If the flow definition is an import string,
        it will automatically load the import string and return the full flow.

        :return: A dict of the Automate Flow definition
        """
        try:
            if isinstance(self.flow_definition, dict):
                prev_state_name = None
                prev_state_data = None

                for state_name, state_data in self.flow_definition["States"].items():
                    if "provenance" in state_name:
                        state_data["ResultPath"] = f"$.{state_name}"
                        transfer_parameters = state_data["Parameters"]
                        transfer_items = transfer_parameters["transfer_items"]

                        # Source and destination endpoints may be set dynamcially

                        # Transfer items (Distriubted Step Crates) are always
                        # named {task_id}.crate

                        transfer_items[0].pop("recursive.$", None)
                        transfer_items[0]["recursive"] = True

                        transfer_items[0].pop("source_path.$", None)
                        transfer_items[0]["source_path.="] = f"`{prev_state_data["ResultPath"]}.details.results[0].task_id` + '.crate'"
                        transfer_items[0].pop("destination_path.$", None)
                        transfer_items[0]['destination_path.='] = f"`$.input._provenance_crate_destination_directory` + '/' + `{prev_state_data["ResultPath"]}.details.results[0].task_id`"

                        transfer_parameters["source_endpoint_id.$"] = "$.input.prov_compute_GCS_id"
                        transfer_parameters["destination_endpoint_id.$"] = "$.input.orchestration_server_endpoint_id"                  
                
                    prev_state_name = state_name
                    prev_state_data = state_data

                return self.flow_definition
            elif isinstance(self.flow_definition, str):
                return self.get_gladier_defaults_cls(
                    self.flow_definition
                ).flow_definition
            else:
                raise gladier.exc.ConfigException(
                    '"flow_definition" must be a dict or an import string '
                    "to a sub-class of type "
                    '"gladier.GladierBaseTool"'
                )
        except AttributeError:
            raise gladier.exc.ConfigException(
                '"flow_definition" was not set on ' f"{self.__class__.__name__}"
            )