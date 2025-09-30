#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# run_analysis.py
#
###############################################################################
# DEPENDENCIES
###############################################################################
import olca_schema as olca


###############################################################################
# DOCUMENTATION
###############################################################################
__doc__ = """
This script includes a function to run the analysis for a product system in openLCA.

**Assumptions**

-   The user has openLCA running with an open database
-   The open database includes databases (e.g., databases imported by the user
    from LCACommons)
-   The user is connected to the openLCA database through IPC
-   The user already used the create_ps function to create a product system

**Logic**

The function takes two main arguments/inputs:

1. client object (IPC client)
2. ps_uuid: the uuid of the product system

The function returns the result.

"""
__all__ = [
    "run_analysis",
]


###############################################################################
# FUNCTIONS
###############################################################################
def run_analysis(client, ps_uuid, impact_method_uuid):
    """
    This function runs the analysis for a product system in openLCA.

    Parameters
    ----------
    client : olca_ipc.Client
        The IPC client object.
    ps_uuid : str
        The UUID of the product system.
    impact_method_uuid : str
        The UUID of the impact method.
    Returns
    -------
    lcia_result : olca_schema.LciaResult
        The LCA result.
    """

    # Define the impact method
    # In this project, the method is defined in a pre-setup database
    # as such, the uuid of the method is less likely to change
    # define method using uuid
    impact_method_ref = client.client.get(olca.ImpactMethod, impact_method_uuid)

    # Define product system object
    ps_ref = client.client.get(olca.ProductSystem, ps_uuid)

    # build the calculation setup
    setup = olca.CalculationSetup()
    setup.allocation = olca.AllocationType.USE_DEFAULT_ALLOCATION
    setup.amount = None # omitted, the code will use the FU
    setup.flow_property = None # omitted, the code will use the FU flow property
    setup.impact_method = impact_method_ref
    setup.nw_set = None
    setup.parameters = None # no parameters are considered in the current model
                            # this can be incorporated in the future
    setup.target = ps_ref
    setup.unit = None # omitted, the code will use the FU unit
    setup.with_costs = False # no costs are considered in the current model
    setup.with_regionalization = False # no regionalization is considered in the current model

    # Run and Generate Result
    result = client.client.calculate(setup)

    return result