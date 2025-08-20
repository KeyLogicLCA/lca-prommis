# This script includes a function that creates an exchange for an elementary flow

# Assumptions:
###################################################################################
# 1. The flow is an elementary flow
# 2. The user knows the flow uuid

# Code logic
###################################################################################

# Get flow
# Get flow property
# Set unit
# Create exchange
# Return exchange    

# Import libraries
###################################################################################
import logging
import olca_schema as olca
import olca_schema.units as o_units

# Main function 
def create_exchange_elementary_flow(client, flow_uuid, unit, amount, is_input) -> olca.Exchange:
	"""Create and return an `olca.Exchange` for an ELEMENTARY_FLOW.

	- `unit`: optional; provide an `olca.Unit` or a unit name. Falls back to the flow's reference unit.
	- `is_input`: bool or 'true'/'false'.
	"""

	# Get flow and make additional checks - it exists and it is an elementary flow
	flow: olca.Flow = client.query(olca.Flow, flow_uuid)
	if flow is None:
		raise ValueError(f"Flow not found: {flow_uuid}")
	if flow.flow_type != olca.FlowType.ELEMENTARY_FLOW:
		raise ValueError("Provided flow is not an ELEMENTARY_FLOW")

	# get reference flow property
		# in olca_schema, the flow property falls under flow.flow_properties
		# the reference flow property is the one with is_ref_flow_property = True
		# this would be the one that help define the unit of the flow (e.g., mass, volume, energy, etc.)
	# flow.flow_properties is a list of FlowPropertyFactors
	# we want the one that is_ref_flow_property = true
	factor = next((f for f in flow.flow_properties if f.is_ref_flow_property), flow.flow_properties[0])
	flow_property = client.query(olca.FlowProperty, factor.flow_property.id)
	if flow_property is None:
		raise ValueError("Failed to resolve FlowProperty for the given flow")

	# set unit
	# if we pass the unit as a string, we need to resolve it to the unit object
	# the reason why we have the _resolve_unit function 
	# if we pass the unit as an object, we can use it directly
	# but the challenge is that the unit object is having have an olca.Unit object that 
	# 	belongs to the same unit group as the flow’s (reference) flow property

	
	# create exchange
	exchange = client.make_exchange() 
	exchange.flow = flow
		# set the FlowProperty reference on the exchange
	exchange.flow_property = flow_property.to_ref() if hasattr(flow_property, "to_ref") else flow_property
	exchange.unit = o_units.unit_ref(unit)
	exchange.amount = float(amount)
	exchange.is_input = is_input

	return exchange


