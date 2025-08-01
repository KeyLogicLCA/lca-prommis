#!/usr/bin/env python3
"""
Flow Manager Module

This module provides functionality for creating, finding, and managing flows in openLCA.
"""

import logging
from typing import List, Optional, Tuple
import pandas as pd

# Import the required modules
try:
    from olca_ipc import Client
    import olca_schema as olca
except ImportError as e:
    logging.error(f"Failed to import required packages: {e}")
    raise

from .olca_utils import generate_id, find_entity_by_name

logger = logging.getLogger(__name__)


def get_existing_flows(client: Client) -> List[olca.Flow]:
    """
    Get all existing flows from the database using efficient descriptor-based retrieval.
    
    Args:
        client (Client): The openLCA IPC client
    
    Returns:
        List[olca.Flow]: List of existing flows
    """
    try:
        flow_descriptors = client.get_descriptors(olca.Flow)
        flows = []
        
        if not flow_descriptors:
            logger.info("No flows found in database")
            return []
        
        print(f"   📊 Retrieving {len(flow_descriptors)} flows...")
        
        for descriptor in flow_descriptors:
            try:
                flow = client.get(olca.Flow, descriptor.id)
                if flow:
                    flows.append(flow)
            except Exception as e:
                logger.warning(f"Could not retrieve flow {descriptor.id}: {e}")
                continue
                
        return flows
    except Exception as e:
        logger.warning(f"Could not retrieve existing flows: {e}")
        return []


def search_flows_by_name(client: Client, flow_name: str) -> List[olca.Flow]:
    """
    Search for flows by name using efficient descriptor-based search.
    
    Args:
        client (Client): The openLCA IPC client
        flow_name (str): Name of the flow to search for
    
    Returns:
        List[olca.Flow]: List of matching flows
    """
    try:
        # Get all flow descriptors first (much faster)
        flow_descriptors = client.get_descriptors(olca.Flow)
        
        if not flow_descriptors:
            logger.info("No flows found in database")
            return []
        
        print(f"   🔍 Searching through {len(flow_descriptors)} flows...")
        matching_flows = []
        flow_name_lower = flow_name.lower()
        
        # Search through descriptors first, then get full objects only for matches
        for descriptor in flow_descriptors:
            try:
                # Ensure both flow name and descriptor name are lowercase for case-insensitive matching
                descriptor_name = descriptor.name.lower() if descriptor.name else ""
                
                # Check if the descriptor name matches (case-insensitive)
                if flow_name_lower in descriptor_name:
                    # Get the full flow object only for matches
                    flow = client.get(olca.Flow, descriptor.id)
                    if flow:
                        matching_flows.append(flow)
                        
            except Exception as e:
                logger.warning(f"Could not retrieve flow {descriptor.id}: {e}")
                continue
                
        return matching_flows
        
    except Exception as e:
        logger.warning(f"Could not search for flows by name: {e}")
        return []


def find_compatible_flow_pattern(client: Client, unit_name: str) -> Tuple[Optional[olca.Flow], Optional[olca.Unit], Optional[olca.FlowProperty]]:
    """
    Find an existing flow with a compatible unit to use as a template pattern.
    This avoids @type issues by using existing relationships.
    
    Args:
        client (Client): The openLCA IPC client
        unit_name (str): The unit name to find a compatible pattern for
    
    Returns:
        Tuple[Optional[olca.Flow], Optional[olca.Unit], Optional[olca.FlowProperty]]: 
        Template flow, unit, and flow property, or (None, None, None) if not found
    """
    try:
        # Get existing processes with exchanges (these have working flow-unit-property relationships)
        process_descriptors = client.get_descriptors(olca.Process)
        
        unit_name_lower = unit_name.lower()
        
        # Define unit compatibility groups
        unit_groups = {
            'mass': ['kg', 'g', 'gram', 'kilogram', 'tonne', 'ton', 'pound', 'lb'],
            'energy': ['j', 'joule', 'kj', 'mj', 'kwh', 'btu', 'cal', 'kcal'],
            'volume': ['l', 'liter', 'litre', 'm3', 'cubic', 'gallon', 'gal', 'ml'],
            'area': ['m2', 'm²', 'sq', 'square', 'ha', 'acre'],
            'length': ['m', 'meter', 'metre', 'km', 'cm', 'mm', 'ft', 'inch']
        }
        
        # Find which group this unit belongs to
        target_group = None
        for group_name, units in unit_groups.items():
            if any(u in unit_name_lower for u in units):
                target_group = group_name
                break
        
        print(f"   🔍 Looking for {target_group or 'any'} unit pattern for: {unit_name}")
        
        # Search through processes to find one with a compatible exchange
        for proc_desc in list(process_descriptors)[:50]:  # Check first 50 processes
            try:
                process = client.get(olca.Process, proc_desc.id)
                
                if not (hasattr(process, 'exchanges') and process.exchanges):
                    continue
                
                for exchange in process.exchanges:
                    if not (hasattr(exchange, 'unit') and exchange.unit and 
                           hasattr(exchange, 'flow') and exchange.flow and
                           hasattr(exchange, 'flow_property') and exchange.flow_property):
                        continue
                    
                    exchange_unit_name = exchange.unit.name.lower()
                    
                    # Check for exact match first
                    if exchange_unit_name == unit_name_lower:
                        print(f"   ✅ Found exact unit match: {exchange.unit.name}")
                        return exchange.flow, exchange.unit, exchange.flow_property
                    
                    # Check for group compatibility
                    if target_group:
                        compatible_units = unit_groups[target_group]
                        if any(u in exchange_unit_name for u in compatible_units):
                            print(f"   ✅ Found compatible unit: {exchange.unit.name} (group: {target_group})")
                            return exchange.flow, exchange.unit, exchange.flow_property
                            
            except Exception as e:
                continue
        
        # If no compatible pattern found, use the first available exchange
        print(f"   ⚠️  No compatible unit found, using first available pattern...")
        for proc_desc in list(process_descriptors)[:10]:
            try:
                process = client.get(olca.Process, proc_desc.id)
                if (hasattr(process, 'exchanges') and process.exchanges and 
                    hasattr(process.exchanges[0], 'flow') and process.exchanges[0].flow and
                    hasattr(process.exchanges[0], 'unit') and process.exchanges[0].unit and
                    hasattr(process.exchanges[0], 'flow_property') and process.exchanges[0].flow_property):
                    
                    exchange = process.exchanges[0]
                    print(f"   ✅ Using fallback pattern: {exchange.unit.name}")
                    return exchange.flow, exchange.unit, exchange.flow_property
            except:
                continue
                
        return None, None, None
        
    except Exception as e:
        print(f"   ❌ Error finding compatible flow pattern: {e}")
        return None, None, None


def create_flow_from_dataframe_row(client: Client, row: pd.Series, existing_flows: List[olca.Flow]) -> Optional[olca.Flow]:
    """
    Create a flow from a DataFrame row or find existing one using efficient search.
    
    Args:
        client (Client): The openLCA IPC client
        row (pd.Series): DataFrame row containing flow information
        existing_flows (List[olca.Flow]): List of existing flows
    
    Returns:
        Optional[olca.Flow]: Created or found flow
    """
    flow_name = row.get('Flow_Name', row.get('Name', 'Unknown Flow'))
    
    # Try to find existing flow in the provided list first
    existing_flow = find_entity_by_name(existing_flows, flow_name, "flow")
    if existing_flow:
        logger.info(f"Using existing flow: {flow_name}")
        return existing_flow
    
    # If not found in provided list, search the database efficiently
    print(f"   🔍 Searching for existing flow: {flow_name}")
    matching_flows = search_flows_by_name(client, flow_name)
    
    if matching_flows:
        # Use the first exact match if available, otherwise the first match
        exact_match = None
        for flow in matching_flows:
            if flow.name.lower() == flow_name.lower():
                exact_match = flow
                break
        
        selected_flow = exact_match if exact_match else matching_flows[0]
        logger.info(f"✅ Found existing flow: {selected_flow.name}")
        return selected_flow
    
    # Create new flow if not found
    flow_type = row.get('Flow_Type', 'PRODUCT_FLOW')
    if flow_type.upper() == 'ELEMENTARY_FLOW':
        flow_type_enum = olca.FlowType.ELEMENTARY_FLOW
    else:
        flow_type_enum = olca.FlowType.PRODUCT_FLOW
    
    # For elementary flows, we need to set up flow properties to avoid unit errors
    if flow_type_enum == olca.FlowType.ELEMENTARY_FLOW:
        # Find a compatible flow pattern to copy flow properties from
        template_flow, template_unit, template_flow_property = find_compatible_flow_pattern(client, row.get('LCA_Unit', 'kg'))
        
        if template_flow and hasattr(template_flow, 'flow_properties') and template_flow.flow_properties:
            new_flow = olca.Flow(
                id=generate_id("flow"),
                name=flow_name,
                description=row.get('Description', f'Elementary flow: {flow_name}'),
                flow_type=flow_type_enum,
                flow_properties=template_flow.flow_properties  # Copy working flow properties
            )
        else:
            # Fallback: create without flow properties (will be handled by find_compatible_flow_pattern later)
            new_flow = olca.Flow(
                id=generate_id("flow"),
                name=flow_name,
                description=row.get('Description', f'Elementary flow: {flow_name}'),
                flow_type=flow_type_enum
            )
    else:
        # Product flows - keep original behavior
        new_flow = olca.Flow(
            id=generate_id("flow"),
            name=flow_name,
            description=row.get('Description', f'Flow: {flow_name}'),
            flow_type=flow_type_enum
        )
    
    try:
        created_flow = client.put(new_flow)
        logger.info(f"✅ Created new flow: {flow_name}")
        return created_flow
    except Exception as e:
        logger.warning(f"Failed to create flow {flow_name}: {e}")
        # Create a basic flow as fallback
        try:
            basic_flow = olca.Flow(
                id=generate_id("flow"),
                name=flow_name,
                description=row.get('Description', f'Basic flow: {flow_name}'),
                flow_type=flow_type_enum
            )
            # Don't try to save to database if it fails
            logger.info(f"✅ Created basic flow: {flow_name} (not saved to database)")
            return basic_flow
        except Exception as e2:
            logger.error(f"Failed to create even basic flow {flow_name}: {e2}")
            return None