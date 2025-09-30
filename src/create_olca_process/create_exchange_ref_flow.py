#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# create_exchange_ref_flow.py
#
###############################################################################
# DEPENDENCIES
###############################################################################
import uuid

import olca_schema as olca
import olca_schema.units as o_units

from src.create_olca_process.search_flows_only import search_and_select_flows


###############################################################################
# DOCUMENTATION
###############################################################################
__doc__ = """
This script includes the function to create an exchange for a reference flow when creating a new process.
The difference between this function and the one created in
create_new_process.py is the following:

1.  The original function in create_new_process.py creates a new flow, makes an
    exchange out of it and sets it as quantitative ref flow.
2.  This function creates an exchange using an existing flow and sets it as
    quantitative reference.

**Assumptions**

-   The user already searched and selected a flow.
-   The user knows the uuids of the flow.
-   The flow amount, unit, is_input, is_reference are extracted from the
    prommis results data frame.

**Logic**

1.  The function gets the flow using the provided uuid.
2.  The function gets the reference flow property
3.  Use the make_exchange function to create the exchange
4.  Assign flow to exchange
5.  Assign flow property
6.  Assign unit using the o_units from olca_schema.units --> string unit from
    prommis results data frame
7.  Assign amount and is_input --> amount from prommis results df
8.  Set as quantitative flow
9.  Return the exchange
"""
__all__ = [
    "create_exchange_ref_existing_flow",
    "create_exchange_ref_flow",
    "create_exchange_ref_new_flow",
    "find_flow_property_for_unit",
    "generate_id",
]


###############################################################################
# FUNCTIONS
###############################################################################
def create_exchange_ref_flow(client,flowName, amount, unit, isInput, isRef):
    """Request from user the new flow or existing flow for the reference exchange.
    """
    # Get input from user
    print (
        "Do you want to select an existing quantitative reference flow or "
        "create a new one?"
    )
    print ("1. Select existing flow")
    print ("2. Create new flow")
    choice = input("Enter your choice (1 or 2): ")
    if choice == "1":
        flow_uuid = search_and_select_flows(keywords=None, client=client)
        return create_exchange_ref_existing_flow(client, flow_uuid, amount, unit)
    elif choice == "2":
        return create_exchange_ref_new_flow(client, flowName, amount, unit, isInput, isRef)
    else:
        raise ValueError("Invalid choice")


def create_exchange_ref_existing_flow(client, flow_uuid, amount, unit):
    """
    Create and return an 'olca.Exchange' for PRODUCT or WASTE flows.

    Parameters
    ----------
    client : olca.Client
        An olca client object.
    flow_uuid : str
        The uuid of the flow.
    provider_uuid : str
        The uuid of the process associated with the flow.
    amount : float
        The amount of the flow.
    unit : str
        The unit of the flow.
    is_input : bool
        Whether the flow is an input or output.

    Returns
    -------
    olca-schema.Exchange
    """
    # Get flow and make additional checks
    # - it exists and it is a product or waste flow
    flow = client.query(olca.Flow, flow_uuid) # returns a olca.Flow object
    if flow is None:
        raise ValueError(f"Flow not found: {flow_uuid}")

    # Get reference flow property
    factor = next((f for f in flow.flow_properties if f.is_ref_flow_property), flow.flow_properties[0])
    flow_property = o_units.property_ref(unit)
    if flow_property is None:
        flow_property = o_units.property_ref(unit.lower())
    if flow_property is None:
        raise ValueError(
            "The flow property is not found in the flow. "
            "Adjust your unit or select another flow"
        )

    # Create exchange
    exchange = client.make_exchange()
    exchange.flow = flow
    exchange.flow_property = flow_property.to_ref() if hasattr(flow_property, "to_ref") else flow_property
    exchange.unit = o_units.unit_ref(unit)
    if exchange.unit is None:
        exchange.unit = o_units.unit_ref(unit.lower())
    exchange.amount = amount
    exchange.is_input = False
    exchange.is_quantitative_reference = True

    return exchange


def create_exchange_ref_new_flow(client,
                                 flowName,
                                 amount,
                                 unit,
                                 isInput,
                                 isRef):
    """Create exchange for reference flow using a new flow."""
    # Get unit object from unit name passed in the function
    unit_obj = o_units.unit_ref(unit)
    if unit_obj is None:
        unit_obj = o_units.unit_ref(unit.lower())

    # Find a flow property that contains this unit.
    flow_property = find_flow_property_for_unit(client, unit_obj)
    if flow_property is None:
        raise ValueError(
            f"Could not find a flow property for unit '{unit_obj.name}' in "
            "the openLCA database"
        )

    # Create reference to the flow property.
    flow_property_ref = olca.Ref(
        id=flow_property.id,
        name=flow_property.name
    )

    # Create flow property factor with proper reference.
    ex_flow_property_factor = olca.FlowPropertyFactor(
        conversion_factor = amount,
        is_ref_flow_property = True,
        flow_property = flow_property_ref
    )

    # Create flow with proper flow properties.
    ex_flow = olca.Flow(
        id = generate_id(),
        name = flowName,
        description = f"Product flow for {flowName}",
        flow_type = olca.FlowType.PRODUCT_FLOW,
        flow_properties = [ex_flow_property_factor]
    )

    # Save the flow to the database first.
    saved_flow = client.client.put(ex_flow)
    print(f"Created flow: {saved_flow.name} with ID: {saved_flow.id}")

    # Create a flow reference for the exchange.
    flow_ref = olca.Ref(
        id=saved_flow.id,
        name=saved_flow.name
    )

    # Use the saved flow reference to create the exchange.
    exchange = olca.Exchange(
        flow = flow_ref,
        flow_property = ex_flow_property_factor,
        unit = unit_obj,
        amount = 1.0,
        is_input = isInput,
        is_quantitative_reference = isRef
    )

    return exchange


def find_flow_property_for_unit(client, unit_obj):
    """
    Find a flow property that contains the given unit.

    Parameters
    ----------
    client: NetlOlca
        Client object.
    unit_obj: olca-schema.Unit
        The unit object from which to find a flow property.

    Returns
    -------
    olca.FlowProperty
        A flow property containing this unit or None if not found.
    """
    try:
        # Get all flow properties using NetlOlca's method
        flow_properties = client.get_all(olca.FlowProperty)

        # Search through flow properties to find one that has the unit in its
        # unit group
        for flow_property in flow_properties:
            if (hasattr(flow_property, 'unit_group')
                    and flow_property.unit_group):
                # Get the unit group
                if hasattr(flow_property.unit_group, 'id'):
                    unit_group = client.client.get(
                        olca.UnitGroup,
                        flow_property.unit_group.id
                    )
                    if hasattr(unit_group, 'units') and unit_group.units:
                        for unit in unit_group.units:
                            if hasattr(unit, 'id') and hasattr(unit_obj, 'id'):
                                if unit.id == unit_obj.id:
                                    return flow_property
                            elif (hasattr(unit, 'name')
                                    and hasattr(unit_obj, 'name')):
                                if unit.name == unit_obj.name:
                                    return flow_property
    except Exception as e:
        print(f"Error finding flow property for unit: {e}")

    return None


def generate_id(prefix: str = "entity") -> str:
    """
    Generate a unique ID for openLCA entities.

    Parameters
    ----------
    prefix : str
        Prefix for the ID (e.g., 'process', 'flow', 'unit').
        Note: prefix is ignored to comply with database VARCHAR(36) limit.

    Returns
    -------
    str
        Unique ID (36-character UUID string).
    """
    return str(uuid.uuid4())
