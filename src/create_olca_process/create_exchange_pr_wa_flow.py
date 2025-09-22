# This script includes the function to create exchanges for product/waste flows

# Assumptions:
###################################################################################
# 1. The flow is a product or waste flow
# 2. The user knows the uuids of the flow and its associated process
# 3. the flow amount, unit, is_input, is_reference are extracted from the prommis results df

# Code logic
###################################################################################

# 1. The function gets the flow using the provided uuid
# 2. The function gets the reference flow property
# 3. Use the make_exchange function to create the exchange
# 4. assign flow to exchange
# 5. assign flow property
# 6. assign unit using the o_units from olca_schema.units --> string unit from prommis results df
# 7. assign amount and is_input --> amount from prommis results df
# 8. assign provider using the uuid of the process associated with the flow
# 9. return the exchange

# Import libraries
###################################################################################
import logging
import olca_schema as olca
import olca_schema.units as o_units

# Main function
def create_exchange_pr_wa_flow(client, flow_uuid, provider_uuid, amount, unit, is_input):
    """
    Create and return an 'olca.Exchange' for PRODUCT or WASTE flows

    Parameters
    ----------
    client : olca.Client - An olca client object.
    flow_uuid : str - The uuid of the flow.
    provider_uuid : str - The uuid of the process associated with the flow.
    amount : float - The amount of the flow.
    unit : str - The unit of the flow.
    is_input : bool - Whether the flow is an input or output.
    """

    # Get flow and make additional checks - it exists and it is a product or waste flow
    flow = client.query(olca.Flow, flow_uuid) # returns a olca.Flow object
    if flow is None:
        raise ValueError(f"Flow not found: {flow_uuid}")
    if flow.flow_type != olca.FlowType.PRODUCT_FLOW and flow.flow_type != olca.FlowType.WASTE_FLOW:
        raise ValueError("Provided flow is not a PRODUCT or WASTE flow")

    # Get reference flow property
    factor = next((f for f in flow.flow_properties if f.is_ref_flow_property), flow.flow_properties[0])
    flow_property = o_units.property_ref(unit)
    if flow_property is None:
        raise ValueError("Failed to resolve FlowProperty for the given flow")
    
    # create exchange
    exchange = client.make_exchange()
    exchange.flow = flow
    exchange.flow_property = flow_property.to_ref() if hasattr(flow_property, "to_ref") else flow_property
    exchange.unit = o_units.unit_ref(unit)
    exchange.amount = float(amount)
    exchange.is_input = is_input
    exchange.default_provider = olca.Ref.from_dict({"@type": "Process", "@id": provider_uuid})

    return exchange