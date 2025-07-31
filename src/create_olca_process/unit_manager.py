#!/usr/bin/env python3
"""
Unit Manager Module

This module provides functionality for creating, finding, and managing units in openLCA.
"""

import logging
from typing import List, Optional
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


def get_existing_units(client: Client) -> List[olca.Unit]:
    """
    Get all existing units from the database. Handle @type attribute issues.
    
    Args:
        client (Client): The openLCA IPC client
    
    Returns:
        List[olca.Unit]: List of existing units
    """
    try:
        unit_descriptors = client.get_descriptors(olca.Unit)
        units = []
        
        if not unit_descriptors:
            logger.info("No units found in database (or @type attribute issue)")
            return []
        
        print(f"   📊 Retrieving {len(unit_descriptors)} units...")
        
        for descriptor in unit_descriptors:
            try:
                unit = client.get(olca.Unit, descriptor.id)
                if unit:
                    units.append(unit)
            except Exception as e:
                logger.warning(f"Could not retrieve unit {descriptor.id}: {e}")
                continue
                
        return units
    except Exception as e:
        logger.warning(f"Could not retrieve existing units (@type issue): {e}")
        # Return empty list - the unit creation functions will handle this
        return []


def get_common_unit_suggestions(unit_name: str) -> List[str]:
    """
    Get common unit name suggestions based on the requested unit.
    
    Args:
        unit_name (str): The requested unit name
    
    Returns:
        List[str]: List of common unit names that might exist in the database
    """
    unit_lower = unit_name.lower()
    
    # Common unit mappings
    unit_mappings = {
        'kg': ['kg', 'kilogram', 'Kg', 'Kilogram'],
        'g': ['g', 'gram', 'Gram'],
        'l': ['L', 'l', 'liter', 'litre', 'Liter', 'Litre'],
        'liter': ['L', 'l', 'liter', 'litre', 'Liter', 'Litre'],
        'litre': ['L', 'l', 'liter', 'litre', 'Liter', 'Litre'],
        'm': ['m', 'meter', 'metre', 'Meter', 'Metre'],
        'm2': ['m2', 'm²', 'square meter', 'square metre'],
        'm3': ['m3', 'm³', 'cubic meter', 'cubic metre'],
        'kwh': ['kWh', 'kw*h', 'kilowatt hour', 'kilowatt-hour'],
        'mj': ['MJ', 'megajoule', 'Megajoule'],
        'tonne': ['tonne', 'ton', 'metric ton', 't'],
        't': ['t', 'tonne', 'ton', 'metric ton'],
        'piece': ['p', 'piece', 'pieces', 'unit', 'item'],
        'p': ['p', 'piece', 'pieces', 'unit', 'item'],
    }
    
    # Find suggestions for the unit
    for key, suggestions in unit_mappings.items():
        if unit_lower == key or unit_lower in suggestions:
            return suggestions
    
    # If no specific mapping found, return variations of the input
    return [unit_name, unit_name.lower(), unit_name.upper(), unit_name.capitalize()]


def search_units_by_name(client: Client, unit_name: str) -> List[olca.Unit]:
    """
    Search for units by name using efficient descriptor-based search.
    Falls back to trying common unit names if @type errors occur.
    
    Args:
        client (Client): The openLCA IPC client
        unit_name (str): The unit name to search for
    
    Returns:
        List[olca.Unit]: List of matching units
    """
    matching_units = []
    
    try:
        unit_descriptors = client.get_descriptors(olca.Unit)
        unit_name_lower = unit_name.lower()
        
        for descriptor in unit_descriptors:
            if descriptor.name:
                descriptor_name_lower = descriptor.name.lower()
                # Smart sorting: exact match, starts with, contains
                if (descriptor_name_lower == unit_name_lower or
                    descriptor_name_lower.startswith(unit_name_lower) or
                    unit_name_lower in descriptor_name_lower):
                    
                    try:
                        unit = client.get(olca.Unit, descriptor.id)
                        if unit:
                            matching_units.append(unit)
                    except Exception as e:
                        logger.warning(f"Could not retrieve unit {descriptor.name}: {e}")
                        continue
                        
    except Exception as e:
        logger.warning(f"Could not search units (@type issue): {e}")
        # Fallback: try to find units by guessing common names
        print(f"   ⚠️  Unit search failed due to @type error.")
        print(f"   🔍 Trying alternative approach...")
        
        suggestions = get_common_unit_suggestions(unit_name)
        for suggestion in suggestions:
            try:
                # Try to find the unit by name directly (this might work even if descriptors don't)
                unit = client.get(olca.Unit, name=suggestion)
                if unit and unit not in matching_units:
                    matching_units.append(unit)
                    print(f"   ✅ Found unit by name: {unit.name}")
            except Exception:
                continue
        
        if not matching_units:
            print(f"   ❌ Could not find any units using alternative methods")
    
    # Sort results: exact match first, then starts with, then contains
    def sort_key(unit):
        name_lower = unit.name.lower() if unit.name else ""
        if name_lower == unit_name.lower():
            return 0  # Exact match
        elif name_lower.startswith(unit_name.lower()):
            return 1  # Starts with
        else:
            return 2  # Contains
    
    matching_units.sort(key=sort_key)
    return matching_units


def find_units_in_flow(client: Client, flow: olca.Flow, requested_unit_name: str) -> List[olca.Unit]:
    """
    Find all available units in a flow's unit group.
    
    Args:
        client (Client): The openLCA IPC client
        flow (olca.Flow): The flow to search for units
        requested_unit_name (str): The requested unit name (for matching)
    
    Returns:
        List[olca.Unit]: List of available units in the flow
    """
    available_units = []
    requested_unit_lower = requested_unit_name.lower()
    
    try:
        if hasattr(flow, 'flow_properties') and flow.flow_properties:
            fp_factor = flow.flow_properties[0]
            flow_property_ref = fp_factor.flow_property
            
            print(f"   🔍 Debug: flow_property_ref = {flow_property_ref}")
            
            # The flow_property might be just a Ref object, so fetch the full object
            if hasattr(flow_property_ref, 'id'):
                try:
                    print(f"   🔄 Fetching full flow property from database...")
                    full_flow_property = client.get(olca.FlowProperty, flow_property_ref.id)
                    
                    if full_flow_property and hasattr(full_flow_property, 'unit_group') and full_flow_property.unit_group:
                        print(f"   🔄 Fetching full unit group from database...")
                        full_unit_group = client.get(olca.UnitGroup, full_flow_property.unit_group.id)
                        
                        if full_unit_group and hasattr(full_unit_group, 'units') and full_unit_group.units:
                            units_in_flow = full_unit_group.units
                            
                            # Extract actual unit objects
                            for unit_item in units_in_flow:
                                # Handle both Unit objects and UnitGroupFactor objects
                                if hasattr(unit_item, 'unit'):
                                    # This is a UnitGroupFactor
                                    actual_unit = unit_item.unit
                                else:
                                    # This is a Unit object directly
                                    actual_unit = unit_item
                                
                                if actual_unit and hasattr(actual_unit, 'name'):
                                    available_units.append(actual_unit)
                            
                            # Sort units: exact match first, then reference unit, then alphabetical
                            def sort_key(unit):
                                unit_name_lower = unit.name.lower() if unit.name else ""
                                if unit_name_lower == requested_unit_lower:
                                    return (0, unit_name_lower)  # Exact match first
                                # Try to identify reference unit (this is tricky without the factor)
                                elif 'reference' in unit_name_lower:
                                    return (1, unit_name_lower)
                                else:
                                    return (2, unit_name_lower)
                            
                            available_units.sort(key=sort_key)
                        else:
                            print(f"   ⚠️  Unit group has no units")
                    else:
                        print(f"   ⚠️  Flow property has no unit group")
                except Exception as e:
                    print(f"   ⚠️  Could not fetch flow property or unit group: {e}")
            else:
                print(f"   ⚠️  Flow property has no ID")
    except Exception as e:
        print(f"   ❌ Error finding units in flow: {e}")
    
    return available_units


def find_unit_in_flow_by_name(client: Client, flow: olca.Flow, unit_name: str, flow_name: str) -> Optional[olca.Unit]:
    """
    Find a specific unit in a flow's unit group, with user selection if needed.
    
    Args:
        client (Client): The openLCA IPC client
        flow (olca.Flow): The flow to search in
        unit_name (str): The unit name to find
        flow_name (str): The flow name (for display)
    
    Returns:
        Optional[olca.Unit]: Found unit or None
    """
    unit_name_lower = unit_name.lower()
    
    # First try to find exact match in flow's unit group
    if hasattr(flow, 'flow_properties') and flow.flow_properties:
        fp_factor = flow.flow_properties[0]
        flow_property = fp_factor.flow_property
        
        if (hasattr(flow_property, 'unit_group') and 
            flow_property.unit_group and 
            hasattr(flow_property.unit_group, 'units') and 
            flow_property.unit_group.units):
            
            # Try to find exact match first
            for unit_factor in flow_property.unit_group.units:
                if (hasattr(unit_factor, 'unit') and 
                    unit_factor.unit and 
                    unit_factor.unit.name.lower() == unit_name_lower):
                    print(f"   ✅ Found matching unit: {unit_factor.unit.name}")
                    return unit_factor.unit
            
            # If no exact match, use reference unit
            for unit_factor in flow_property.unit_group.units:
                if (hasattr(unit_factor, 'is_reference_unit') and 
                    unit_factor.is_reference_unit and
                    hasattr(unit_factor, 'unit')):
                    print(f"   ✅ Using reference unit: {unit_factor.unit.name}")
                    return unit_factor.unit
            
            # If still no unit, use first available
            if flow_property.unit_group.units:
                first_unit_factor = flow_property.unit_group.units[0]
                if hasattr(first_unit_factor, 'unit') and first_unit_factor.unit:
                    print(f"   ✅ Using first available unit: {first_unit_factor.unit.name}")
                    return first_unit_factor.unit
    
    # If simple search failed, try comprehensive search with user selection
    print(f"⚠️  Could not find unit '{unit_name}' for flow: {flow.name}")
    print(f"   📋 Available units in this flow:")
    
    available_units = find_units_in_flow(client, flow, unit_name)
    
    if available_units:
        from .user_interface import show_unit_selection_in_flow_menu
        return show_unit_selection_in_flow_menu(flow_name, unit_name, available_units)
    else:
        print(f"   ❌ No valid units found in flow")
        return None


def create_unit_from_dataframe_row(client: Client, row: pd.Series, existing_units: List[olca.Unit]) -> Optional[olca.Unit]:
    """
    Create a unit from a DataFrame row or find existing one using efficient search.
    
    Args:
        client (Client): The openLCA IPC client
        row (pd.Series): DataFrame row containing unit information
        existing_units (List[olca.Unit]): List of existing units
    
    Returns:
        Optional[olca.Unit]: Created or found unit
    """
    unit_name = row.get('LCA_Unit', row.get('Unit', 'kg'))
    
    # Try to find existing unit in the provided list first
    existing_unit = find_entity_by_name(existing_units, unit_name, "unit")
    if existing_unit:
        logger.info(f"Using existing unit: {unit_name}")
        return existing_unit
    
    # Search for units in the database
    print(f"   🔍 Searching for existing units matching: {unit_name}")
    matching_units = search_units_by_name(client, unit_name)
    
    if matching_units:
        # Use the first exact match if available, otherwise the first match
        exact_match = None
        for unit in matching_units:
            if unit.name.lower() == unit_name.lower():
                exact_match = unit
                break
        
        selected_unit = exact_match if exact_match else matching_units[0]
        logger.info(f"✅ Found existing unit: {selected_unit.name}")
        return selected_unit
    
    # Create new unit if no matches found
    print(f"   🔨 Creating new unit: {unit_name}")
    
    try:
        # Create new unit
        new_unit = olca.Unit(
            id=generate_id("unit"),
            name=unit_name,
            description=f"Unit: {unit_name}"
        )
        
        try:
            created_unit = client.put(new_unit)
            logger.info(f"✅ Created new unit: {unit_name}")
            return created_unit
        except Exception as e:
            logger.warning(f"Failed to save unit {unit_name} to database: {e}")
            # Return the in-memory unit object anyway
            logger.info(f"✅ Created unit object: {unit_name} (not saved to database)")
            return new_unit
                
    except Exception as e:
        logger.error(f"Failed to create unit {unit_name}: {e}")
        return None