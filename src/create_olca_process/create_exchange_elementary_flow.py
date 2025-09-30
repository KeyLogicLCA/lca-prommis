#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# create_exchange_elementary_flow.py
#
###############################################################################
# DEPENDENCIES
###############################################################################
import olca_schema as olca
import olca_schema.units as o_units


###############################################################################
# DOCUMENTATION
###############################################################################
__doc__ = """
This script includes a function that creates an exchange for an elementary flow.

**Assumptions**

-    The flow is an elementary flow.
-    The user knows the flow uuid.

**Logic**

-    Get flow
-    Get flow property
-    Set unit
-    Create exchange
-    Return exchange
"""
__all__ = [
    "create_exchange_elementary_flow",
]


###############################################################################
# FUNCTIONS
###############################################################################
def create_exchange_elementary_flow(client,
                                    flow_uuid,
                                    unit,
                                    amount,
                                    is_input) -> olca.Exchange:
    """Create and return an `olca.Exchange` for an ELEMENTARY_FLOW.

    Parameters
    ----------
    client : NetlOlca
        An instance of NetlOlca class.
    flow_uuid : str
        Flow universally unique identifier.
    unit : olca.Unit, str
        A Unit class instance or unit name.
        Falls bac to the flow's reference unit.
    amount : int, float
        Numeric flow amount.
    is_input : bool
        Whether the flow is an input or output.

    Returns
    -------
    olca.Exchange
        Exchange object.

    Raises
    ------
    ValueError
        Failed to find flow or flow property in openLCA database or the flow
        type is not an elementary flow type.
    """
    # Get flow and make additional checks
    # - it exists and it is an elementary flow
    flow: olca.Flow = client.query(olca.Flow, flow_uuid)
    if flow is None:
        raise ValueError(f"Flow not found: {flow_uuid}")
    if flow.flow_type != olca.FlowType.ELEMENTARY_FLOW:
        raise ValueError("Provided flow is not an ELEMENTARY_FLOW")

    # Get reference flow property.
    # In olca_schema, the flow property falls under flow.flow_properties
    # the reference flow property is the one with is_ref_flow_property = True
    # this would be the one that help define the unit of the flow (e.g., mass,
    # volume, energy, etc.), and flow.flow_properties is a list of
    # FlowPropertyFactors we want the one that is_ref_flow_property = true
    flow_property = o_units.property_ref(unit)
    if flow_property is None:
        flow_property = o_units.property_ref(unit.lower())
    if flow_property is None:
        raise ValueError(
            "The flow property is not found in the flow. "
            "Adjust your unit or select another flow"
        )

    # Set unit.
    # If we pass the unit as a string, we need to resolve it to the unit object.
    # The reason why we have the _resolve_unit function is that if we pass the
    # unit as an object, we can use it directly but the challenge is that the
    # unit object is having have an olca.Unit object that belongs to the same
    # unit group as the flow’s (reference) flow property

    # Create exchange
    exchange = client.make_exchange()
    exchange.flow = flow

	# Set the FlowProperty reference on the exchange
    exchange.flow_property = flow_property
    exchange.unit = o_units.unit_ref(unit)
    if exchange.unit is None:
        exchange.unit = o_units.unit_ref(unit.lower())
    exchange.amount = float(amount)
    exchange.is_input = is_input

    return exchange
