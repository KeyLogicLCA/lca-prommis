#!/usr/bin/env python3
"""
User Interface Module

This module provides user interaction functionality including menus and selection dialogs.
"""

import logging
from typing import List, Optional, Tuple, Union

# Import the required modules
try:
    from olca_ipc import Client
    import olca_schema as olca
except ImportError as e:
    logging.error(f"Failed to import required packages: {e}")
    raise

logger = logging.getLogger(__name__)



def show_process_selection_menu(existing_processes: List[olca.Process]) -> Optional[olca.Process]:
    """
    Show a menu for user to select an existing process.
    
    Args:
        existing_processes (List[olca.Process]): List of existing processes
    
    Returns:
        Optional[olca.Process]: Selected process or None
    """
    if not existing_processes:
        print("No existing processes found in database.")
        return None
    
    print("\n=== Existing Processes ===")
    for i, process in enumerate(existing_processes, 1):
        print(f"{i}. {process.name} (ID: {process.id})")
        if hasattr(process, 'description') and process.description:
            print(f"   Description: {process.description}")
        if hasattr(process, 'exchanges') and process.exchanges:
            print(f"   Exchanges: {len(process.exchanges)}")
        print()
    
    while True:
        try:
            choice = input("Select a process number (or 'q' to quit): ").strip()
            if choice.lower() == 'q':
                return None
            
            choice_num = int(choice)
            if 1 <= choice_num <= len(existing_processes):
                selected_process = existing_processes[choice_num - 1]
                print(f"Selected: {selected_process.name}")
                return selected_process
            else:
                print(f"Please enter a number between 1 and {len(existing_processes)}")
        except ValueError:
            print("Please enter a valid number or 'q' to quit")
        except KeyboardInterrupt:
            print("\nSelection cancelled.")
            return None


def get_user_search_choice(flow_name: str) -> tuple:
    """
    Ask user to choose search type and keywords for a specific flow.
    
    Args:
        flow_name (str): Name of the flow being processed
    
    Returns:
        tuple: (search_type, search_keywords) where search_type is 'process', 'flow', or 'skip'
    """
    print(f"\n🔍 Search Options for Flow: {flow_name}")
    print("💡 Choose search strategy:")
    print("   1. Search in PROCESSES (e.g., 'Coal power plant', 'Steel production')")
    print("   2. Search in FLOWS (e.g., 'CO2', 'Water', 'Electricity')")
    print("   3. Skip this flow")
    print()
    print("💡 Examples:")
    print("   - For 'Coal': Choose PROCESSES (search for coal power plants)")
    print("   - For 'CO2': Choose FLOWS (search for CO2 emissions flows)")
    print("   - For 'Water': Choose FLOWS (search for water flows)")
    
    while True:
        try:
            choice = input(f"\nChoose search type for '{flow_name}' (1=Process, 2=Flow, 3=Skip): ").strip()
            
            if choice == '1':
                search_type = 'process'
                break
            elif choice == '2':
                search_type = 'flow'
                break
            elif choice == '3':
                return ('skip', None)
            else:
                print("Please enter 1, 2, or 3")
                continue
                
        except KeyboardInterrupt:
            return ('skip', None)
    
    # Get search keywords
    print(f"\n🔍 Enter search keywords for {search_type.upper()} search:")
    print(f"Default would be: '{flow_name}'")
    
    while True:
        try:
            user_input = input(f"Enter keywords (or press Enter for '{flow_name}'): ").strip()
            
            if not user_input:
                # Use the original flow name if no input provided
                return (search_type, flow_name)
            else:
                return (search_type, user_input)
                
        except KeyboardInterrupt:
            return ('skip', None)


def show_flow_process_selection_menu(flow_name: str, matching_items: List, search_type: str, search_keywords: str = None) -> Optional[Union[olca.Process, str, Tuple[str, olca.Flow]]]:
    """
    Show a menu for user to select a process or flow for a specific flow.
    
    Args:
        flow_name (str): Name of the flow being processed
        matching_items (List): List of matching processes or flows
        search_type (str): Type of search performed ('process' or 'flow')
        search_keywords (str): Keywords used for search (for display purposes)
    
    Returns:
        Optional[Union[olca.Process, str, Tuple[str, olca.Flow]]]: Selected process, flow type string, or existing flow tuple
    """
    search_display = f" (searched in {search_type}s: '{search_keywords}')" if search_keywords and search_keywords != flow_name else ""
    print(f"\n=== {search_type.title()} Selection for Flow: {flow_name}{search_display} ===")
    
    if not matching_items:
        print(f"No existing {search_type}s found matching '{search_keywords}'")
        print("Options:")
        print("1. Create new elementary flow")
        print("2. Create new product flow")
        print("0. Skip this flow")
        
        while True:
            try:
                choice = input(f"\nSelect option for '{flow_name}' (0-2): ").strip()
                choice_num = int(choice)
                
                if choice_num == 0:
                    print(f"Skipping flow: {flow_name}")
                    return None
                elif choice_num == 1:
                    print(f"Will create new elementary flow: {flow_name}")
                    return "ELEMENTARY_FLOW"
                elif choice_num == 2:
                    print(f"Will create new product flow: {flow_name}")
                    return "PRODUCT_FLOW"
                else:
                    print("Please enter a number between 0 and 2")
                    
            except ValueError:
                print("Please enter a valid number")
            except KeyboardInterrupt:
                print("\nSelection cancelled.")
                return None
    
    print(f"Found {len(matching_items)} {search_type}(s) matching '{search_keywords}':")
    
    if search_type == 'process':
        # Display processes
        for i, process in enumerate(matching_items, 1):
            print(f"\n{i}. {process.name} (ID: {process.id})")
            if hasattr(process, 'description') and process.description:
                print(f"   Description: {process.description}")
            
            # Show the specific exchange for this flow
            if hasattr(process, 'exchanges') and process.exchanges:
                for exchange in process.exchanges:
                    if hasattr(exchange, 'flow') and exchange.flow:
                        if hasattr(exchange.flow, 'name') and exchange.flow.name.lower() == flow_name.lower():
                            direction = "Input" if exchange.is_input else "Output"
                            ref_product = " (Reference Product)" if exchange.is_quantitative_reference else ""
                            print(f"   Exchange: {direction} - {exchange.amount} {exchange.unit.name}{ref_product}")
                            break
    else:
        # Display flows
        for i, flow in enumerate(matching_items, 1):
            print(f"\n{i}. {flow.name} (ID: {flow.id})")
            if hasattr(flow, 'description') and flow.description:
                print(f"   Description: {flow.description}")
            print(f"   Type: {getattr(flow, 'flow_type', 'Unknown')}")
    
    print(f"\n{len(matching_items) + 1}. Create new elementary flow")
    print(f"{len(matching_items) + 2}. Create new product flow")
    print("0. Skip this flow")
    
    while True:
        try:
            choice = input(f"\nSelect option for '{flow_name}' (0-{len(matching_items) + 2}): ").strip()
            
            choice_num = int(choice)
            
            if choice_num == 0:
                print(f"Skipping flow: {flow_name}")
                return None
            elif 1 <= choice_num <= len(matching_items):
                selected_item = matching_items[choice_num - 1]
                if search_type == 'process':
                    print(f"Selected process: {selected_item.name}")
                    return selected_item
                else:
                    print(f"Selected flow: {selected_item.name}")
                    return ("EXISTING_FLOW", selected_item)
            elif choice_num == len(matching_items) + 1:
                print(f"Will create new elementary flow: {flow_name}")
                return "ELEMENTARY_FLOW"
            elif choice_num == len(matching_items) + 2:
                print(f"Will create new product flow: {flow_name}")
                return "PRODUCT_FLOW"
            else:
                print(f"Please enter a number between 0 and {len(matching_items) + 2}")
                
        except ValueError:
            print("Please enter a valid number")
        except KeyboardInterrupt:
            print("\nSelection cancelled.")
            return None


def show_unit_selection_in_flow_menu(flow_name: str, requested_unit_name: str, available_units: List[olca.Unit]) -> Optional[olca.Unit]:
    """
    Show a menu for user to select a unit from a flow's available units.
    
    Args:
        flow_name (str): Name of the flow
        requested_unit_name (str): The originally requested unit name
        available_units (List[olca.Unit]): List of available units in the flow
    
    Returns:
        Optional[olca.Unit]: Selected unit or None if skipped
    """
    if not available_units:
        print(f"   ❌ No units available in flow: {flow_name}")
        return None
    
    print(f"\n📏 Available units in flow '{flow_name}' (requested: '{requested_unit_name}'):")
    
    # Prepare unit options for user selection
    for i, unit in enumerate(available_units):
        ref_text = ""
        # Try to identify if this might be a reference unit
        if hasattr(unit, 'name') and unit.name:
            if any(keyword in unit.name.lower() for keyword in ['reference', 'ref', 'base']):
                ref_text = " (reference)"
            elif unit.name.lower() == requested_unit_name.lower():
                ref_text = " (exact match)"
        
        print(f"     {i}: {unit.name}{ref_text}")
    
    print(f"   {len(available_units)}: Skip this flow")
    
    while True:
        try:
            choice = input(f"Select unit for '{flow_name}' (0-{len(available_units)}): ").strip()
            choice_idx = int(choice)
            
            if choice_idx == len(available_units):
                print(f"   ⏭️  Skipping flow: {flow_name}")
                return None
            elif 0 <= choice_idx < len(available_units):
                selected_unit = available_units[choice_idx]
                print(f"   ✅ Selected unit: {selected_unit.name}")
                return selected_unit
            else:
                print(f"   ❌ Please enter a number between 0 and {len(available_units)}")
                
        except ValueError:
            print("   ❌ Invalid input. Please enter a number.")
            continue
        except KeyboardInterrupt:
            print(f"\n   ⏭️  Skipping flow: {flow_name}")
            return None


def get_yes_no_input(prompt: str) -> bool:
    """
    Get a yes/no input from the user.
    
    Args:
        prompt (str): The prompt to display
    
    Returns:
        bool: True for yes, False for no
    """
    while True:
        try:
            choice = input(f"{prompt} (y/n): ").lower().strip()
            if choice in ['y', 'yes']:
                return True
            elif choice in ['n', 'no']:
                return False
            else:
                print("Please enter 'y' or 'n'")
        except KeyboardInterrupt:
            print("\nOperation cancelled.")
            return False