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
def create_parameter_set (client, process_uuid, ps_uuid, parameter_set_name, description, is_baseline):
    """
    Create a parameter set for a product system in openLCA.
    
    Parameters
    ----------
    client : olca_ipc.Client
        The IPC client object.
    process_uuid : str
        The UUID of the process.
    ps_uuid : str
        The UUID of the product system.
    parameter_set_name : str
        The name of the parameter set.
    description : str
        The description of the parameter set.
    is_baseline : bool
        Whether the parameter set is a baseline parameter set.
    
    Returns
    -------
    parameter_set : olca_schema.ParameterRedefSet
        The parameter set object.
    """
    # get process object
    process_obj = client.query(olca.Process, process_uuid)
    # get reference flow type
    ref_exch = next(x for x in process_obj.exchanges if x.is_quantitative_reference)
    ref_flow_type = ref_exch.flow.flow_type
    context = olca.Ref (id = process_obj.id, 
                        category = process_obj.category if process_obj.category else None, 
                        description=process_obj.description if process_obj.description else None, 
                        flow_type = ref_flow_type, 
                        library=process_obj.library if process_obj.library else None, 
                        location = process_obj.location.name if process_obj.location else None, 
                        name = process_obj.name, 
                        process_type = process_obj.process_type, 
                        ref_unit = ref_exch.flow_property.ref_unit, 
                        ref_type = olca.RefType.Process)
    # loop through parameters in the process
    parameters = []
    for param in process_obj.parameters:
        parameters.append(olca.ParameterRedef(context = context, description = param.description, is_protected = False, name = param.name, uncertainty = param.uncertainty, value = param.value))
    # create parameter set object
    parameter_set = olca.ParameterRedefSet(name = parameter_set_name, description = description, is_baseline = is_baseline, parameters = parameters)
    # save parameter set object
    ps = client.query(olca.ProductSystem, ps_uuid)
    if isinstance (ps.parameter_sets, list):
        ps.parameter_sets.append(parameter_set)
    else:
        ps.parameter_sets = [parameter_set]
    client.client.put(ps)

    return parameter_set

def update_parameter(client, 
                    ps_uuid, 
                    parameter_set_name, 
                    new_parameter_set):
    """
    This function updates the parameter set for a given product system
    
    Code Logic:
    - get ps ref object
    - get parameter set that has name == parameter_set_name
    - from parameter set get parameters list
    - loop through parameters (existing as parameter objects ParameterRedef)
    - for each parameter, update the value - check new value for parameter 
    name in new_parameter_set
    
    Parameters
    ----------
    client : olca_ipc.Client
        The IPC client object.
    ps_uuid : str
        The UUID of the product system.
    parameter_set_name : str
        The name of the parameter set.
    new_parameter_set: df
        A dataframe with the new parameter set.
        Contains at least two columns: parameter_name and parameter_value
    
    Returns
    -------
    None
    """

    # get product system object
    ps_obj = client.query(olca.ProductSystem, ps_uuid)
    # get parameter set that has a name == parameter_set_name
    parameter_set_obj = next(x for x in ps_obj.parameter_sets if x.name == parameter_set_name)
    # loop through parameters in parameter set
    for param in parameter_set_obj.parameters:
        # get parameter name
        param_name = param.name
        # get new parameter value
        param.value = (
            new_parameter_set.loc[
                new_parameter_set['parameter_name'] == param_name,
                'parameter_value'
            ].iloc[0]
            if param_name in new_parameter_set['parameter_name'].values
            # if parameter name is not in new_parameter_set, keep the original value
            else param.value
        )
    
    client.client.put(ps_obj)

    return parameter_set_obj

def run_analysis(client, ps_uuid, impact_method_uuid, parameter_set):
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
    parameter_set : olca_schema.ParameterRedefSet
        The parameter set to be used in the analysis.
    
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
    setup.parameters = parameter_set
    setup.target = ps_ref
    setup.unit = None # omitted, the code will use the FU unit
    setup.with_costs = False # no costs are considered in the current model
    setup.with_regionalization = False # no regionalization is considered in the current model

    # Run and Generate Result
    result = client.client.calculate(setup)

    return result