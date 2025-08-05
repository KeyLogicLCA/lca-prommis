#!/usr/bin/env python3
"""
Process Manager Module

This module provides functionality for creating and managing processes in openLCA.
"""

import logging
from typing import Optional, Union, Tuple
from datetime import datetime
import pandas as pd

# Import the required modules
try:
    from olca_ipc import Client
    import olca_schema as olca
except ImportError as e:
    logging.error(f"Failed to import required packages: {e}")
    raise

from .olca_utils import generate_id
from .flow_manager import find_compatible_flow_pattern, search_flows_by_name
from .dataframe_utils import get_amount_from_dataframe_row
from .search_utils import search_processes_by_keywords
from .user_interface import get_user_search_choice, show_flow_process_selection_menu

logger = logging.getLogger(__name__)




def create_process_from_dataframe_with_selection(
    client: Client,
    df: pd.DataFrame,
    process_name: str,
    process_description: str = ""
) -> Optional[olca.Process]:
    """
    Create a process from DataFrame with user selection for each flow.
    
    Args:
        client (Client): The openLCA IPC client
        df (pd.DataFrame): DataFrame with flow information
        process_name (str): Name for the new process
        process_description (str): Description for the new process
    
    Returns:
        Optional[olca.Process]: Created process
    """

    # Create new process
    process_id = generate_id("process")
    process = olca.Process(
        id=process_id,
        name=process_name,
        description=process_description,
        process_type=olca.ProcessType.UNIT_PROCESS,
        version="1.0.0",
        last_change=datetime.now().isoformat()
    )
    
    exchanges = []
    created_flows = {}  # Cache for created flows
    created_units = {}  # Cache for created units
    
    print(f"\n🔍 Processing {len(df)} flows for process: {process_name}")
    print("=" * 60)
    
    # Process each row in the DataFrame
    for index, row in df.iterrows():
        flow_name = row.get('Flow_Name', row.get('Name', 'Unknown Flow'))
        unit_name = row.get('LCA_Unit', row.get('Unit', 'kg'))
        amount = get_amount_from_dataframe_row(row)
        is_input = row.get('Is_Input', True)
        is_reference = row.get('Reference_Product', False)
        
        print(f"\n📋 Row {index + 1}: {flow_name}")
        print(f"   Unit: {unit_name}, Amount: {amount}")
        print(f"   Type: {'Input' if is_input else 'Output'}")
        
        # Get custom search choice from user (process vs flow search)
        search_type, search_keywords = get_user_search_choice(flow_name)
        
        if search_type == "skip":
            print(f"⏭️  Skipping flow: {flow_name}")
            continue
        
        # Search based on user's choice
        if search_type == 'process':
            print(f"   🔍 Searching for processes containing '{search_keywords}'...")
            matching_items = search_processes_by_keywords(client, search_keywords)
            print(f"   📊 Found {len(matching_items)} matching processes")
        else:  # search_type == 'flow'
            print(f"   🔍 Searching for flows containing '{search_keywords}'...")
            matching_items = search_flows_by_name(client, search_keywords)
            print(f"   📊 Found {len(matching_items)} matching flows")
        
        # Let user select what to do with this flow
        selection = show_flow_process_selection_menu(flow_name, matching_items, search_type, search_keywords)
        
        flow, unit = _process_user_selection(client, selection, flow_name, unit_name, row)
        
        if not flow or not unit:
            print(f"⏭️  Skipping flow: {flow_name}")
            continue
        
        # Create exchange with proper flow property reference
        flow_property = _get_flow_property_for_exchange(flow)
        
        # Create exchange
        exchange = olca.Exchange(
            flow=flow,
            flow_property=flow_property,
            unit=unit,
            amount=amount,
            is_input=is_input,
            is_quantitative_reference=is_reference
        )
        
        # Validate the exchange
        if not flow_property:
            print(f"   ⚠️  Warning: No flow property found for exchange {flow.name} - {unit.name}")
            print(f"   ⚠️  This may cause issues in openLCA")
        else:
            print(f"   ✅ Exchange uses flow property: {flow_property.name if hasattr(flow_property, 'name') else 'Unknown'}")
        
        exchanges.append(exchange)
        print(f"✅ Added exchange: {flow.name} - {amount} {unit.name} ({'Input' if is_input else 'Output'})")
    
    # Add exchanges to process
    process.exchanges = exchanges
    
    # Save process with detailed error information
    print(f"\n💾 Attempting to save process to database...")
    print(f"   Process: {process_name}")
    print(f"   ID: {process_id}")
    print(f"   Exchanges: {len(exchanges)}")
    
    created_process = client.put(process)
    logger.info(f"✅ Successfully created process: {process_name} (ID: {process_id})")
    print(f"✅ Process saved successfully to openLCA database!")
    return created_process



def _process_user_selection(client: Client, selection: Union[str, Tuple, olca.Process, None], 
                           flow_name: str, unit_name: str, row: pd.Series) -> Tuple[Optional[olca.Flow], Optional[olca.Unit]]:
    """
    Process the user's selection and return the appropriate flow and unit.
    
    Args:
        client (Client): The openLCA IPC client
        selection: The user's selection result
        flow_name (str): Name of the flow
        unit_name (str): Name of the unit
        row (pd.Series): DataFrame row
    
    Returns:
        Tuple[Optional[olca.Flow], Optional[olca.Unit]]: Flow and unit objects
    """
    if selection is None:
        return None, None
    
    elif isinstance(selection, str):
        # User chose to create new flow
        flow_type = selection
        print(f"🆕 Creating new {flow_type.lower()}: {flow_name}")
        print(f"   🔧 Using existing flow pattern to avoid @type issues...")
        
        # Find an existing flow with a compatible unit to copy the pattern from
        template_flow, template_unit, template_flow_property = find_compatible_flow_pattern(client, unit_name)
        
        if not template_flow:
            print(f"❌ Could not find compatible flow pattern for unit: {unit_name}")
            return None, None
        
        print(f"   ✅ Found template flow: {template_flow.name}")
        print(f"   ✅ Template unit: {template_unit.name}")
        print(f"   ✅ Template flow property: {template_flow_property.name if hasattr(template_flow_property, 'name') else 'Unknown'}")
        
        # Create new flow using the template pattern
        new_flow = olca.Flow(
            id=generate_id("flow"),
            name=flow_name,
            description=row.get('Description', f'New {flow_type.lower()}: {flow_name}'),
            flow_type=olca.FlowType.ELEMENTARY_FLOW if flow_type == "ELEMENTARY_FLOW" else olca.FlowType.PRODUCT_FLOW
            #flow_properties=template_flow.flow_properties
        )
        
        # Try to save the new flow
        try:
            saved_flow = client.put(new_flow)
            print(f"   ✅ Created new flow: {flow_name}")
            return saved_flow, template_unit
        except Exception as e:
            print(f"   ⚠️  Could not save new flow ({e}), using template flow instead")
            return template_flow, template_unit
    
    elif isinstance(selection, tuple) and selection[0] == "EXISTING_FLOW":
        # User selected an existing flow
        selected_flow = selection[1]
        print(f"✅ Using existing flow: {selected_flow.name}")
        
        # Find the unit in the selected flow
        unit = _find_unit_in_flow(client, selected_flow, unit_name, flow_name)
        
        if not unit:
            print(f"❌ No suitable unit found in flow: {selected_flow.name}")
            return None, None
            
        return selected_flow, unit
    
    elif isinstance(selection, olca.Process):
        # User selected an existing process
        selected_process = selection
        print(f"✅ Using existing process: {selected_process.name}")
        
        flow, unit = _select_flow_from_process(selected_process, flow_name)
        return flow, unit
    
    return None, None


def _find_unit_in_flow(client: Client, flow: olca.Flow, unit_name: str, flow_name: str) -> Optional[olca.Unit]:
    """
    Find a unit in the given flow's unit group.
    
    Args:
        client (Client): The openLCA IPC client
        flow (olca.Flow): The flow to search in
        unit_name (str): The unit name to find
        flow_name (str): The flow name (for display)
    
    Returns:
        Optional[olca.Unit]: Found unit or None
    """
    from .unit_manager import find_unit_in_flow_by_name
    return find_unit_in_flow_by_name(client, flow, unit_name, flow_name)


def _select_flow_from_process(process: olca.Process, flow_name: str) -> Tuple[Optional[olca.Flow], Optional[olca.Unit]]:
    """
    Select a flow from an existing process.
    
    Args:
        process (olca.Process): The process to select from
        flow_name (str): The flow name (for display)
    
    Returns:
        Tuple[Optional[olca.Flow], Optional[olca.Unit]]: Selected flow and unit
    """
    print(f"   🔍 Selecting flow from process: {process.name}")
    
    # Show available flows in the selected process and let user choose
    if hasattr(process, 'exchanges') and process.exchanges:
        available_flows = []
        for exchange in process.exchanges:
            if hasattr(exchange, 'flow') and exchange.flow and hasattr(exchange.flow, 'name'):
                flow_info = {
                    'flow': exchange.flow,
                    'unit': exchange.unit,
                    'exchange': exchange
                }
                available_flows.append(flow_info)
        
        if available_flows:
            print(f"\n📋 Available flows in process '{process.name}':")
            for i, flow_info in enumerate(available_flows):
                unit_name = flow_info['unit'].name if flow_info['unit'] and hasattr(flow_info['unit'], 'name') else "No unit"
                print(f"   {i}: {flow_info['flow'].name} ({unit_name})")
            
            while True:
                try:
                    choice = input(f"Select flow to use for '{flow_name}' (0-{len(available_flows)-1}): ").strip()
                    choice_idx = int(choice)
                    if 0 <= choice_idx < len(available_flows):
                        selected_flow_info = available_flows[choice_idx]
                        flow = selected_flow_info['flow']
                        unit = selected_flow_info['unit']
                        print(f"✅ Selected flow: {flow.name} (will be used for '{flow_name}' in your process)")
                        return flow, unit
                    else:
                        print(f"❌ Please enter a number between 0 and {len(available_flows)-1}")
                except (ValueError, KeyboardInterrupt):
                    print("❌ Invalid input or cancelled.")
                    return None, None
        else:
            print(f"❌ No flows found in selected process '{process.name}'")
            return None, None
    else:
        print(f"❌ Selected process '{process.name}' has no exchanges")
        return None, None


def _get_flow_property_for_exchange(flow: olca.Flow) -> Optional[olca.FlowProperty]:
    """
    Get the appropriate flow property for an exchange.
    
    Args:
        flow (olca.Flow): The flow object
    
    Returns:
        Optional[olca.FlowProperty]: Flow property or None
    """
    if hasattr(flow, 'flow_properties') and flow.flow_properties:
        # Use the reference flow property (the first one or the one marked as reference)
        for fp_factor in flow.flow_properties:
            if hasattr(fp_factor, 'is_ref_flow_property') and fp_factor.is_ref_flow_property:
                return fp_factor.flow_property
        # If no reference found, use the first one
        if flow.flow_properties:
            return flow.flow_properties[0].flow_property
    
    return None