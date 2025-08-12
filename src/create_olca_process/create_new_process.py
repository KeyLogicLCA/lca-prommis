########################################################################################################
# TODO's in this script:
# Writing/editing functions
    # Develop function to create empty process                                              --> DONE (create_empty_process.py in create_new_process.py module)
    # Develop function to create exchange for reference product flow                        --> DONE (create_ref_product_exchange.py in create_new_process.py module)
    # Develop function to search for flows by keywords                                      --> DONE (search_Flows_by_keywords.py in flow_search_function.py module)
    # Develop function to get processes associated with a specific flow                     --> DONE (find_processes_by_flow.py in flow_search_function.py module)
    # Develop function to create exchange for elementary flows                              --> DONE
    # Develop function to create exchange for product/waste flows (given flow + process)
    # Develop function to get flow property of the flow selected by the user
    # Develop function to get the available units for the flow selected by the user
    # Use function from netlolca library to create exchange for the selected flow 
    # Create exchange for the reference product flow using netlolca library

# Testing/debugging
    # Test (and debug if needed): create_empty_process                                      --> DONE   
    # Test (and debug if needed): create_ref_product_exchange                               --> DONE
    # Test (and debug if needed): search_Flows_by_keywords                                  --> DONE (search_Flows_by_keywords.py in flow_search_function.py module)
    # Test (and debug if needed): find_processes_by_flow                                    --> DONE (find_processes_by_flow.py in flow_search_function.py module)
    # Test (and debug if needed): create_exchange_elementary_flow                           --> In progress
    # Test (and debug if needed): show_flow_process_selection_menu
    # Test (and debug if needed): get_user_search_choice
    # Test (and debug if needed): create_new_process
    # Test (and debug if needed): get_flow_properties
    # Test (and debug if needed): get_flow_units
    # Test (and debug if needed): select_flow_property_and_unit

# Other
    # check if openlca has a limit on the number of characters for the name                 TODO 2
    # check if openlca has a limit on the number of characters for the description          TODO 3
    # for elementary flows - Daniel will add uuids for each flow (FEDEFL)                   --> DONE
    # create openLCA database with all the available processes/providers                    TODO 1   

# TODO - 4 for Daniel - TODO Notes
# write a script to:
    # ask the user for a flow name (keyword) and flow type (product, waste) - keyword search
    # the code uses search_Flows_by_keywords.py to search for flows
    # ask the user to pick a flow from the list of matching flows
    # the code uses find_processes_by_flow.py to find processes associated with the selected flow
    # ask the user to pick a process from the list of matching processes
    # IMPORTANT: write this as a function in separate module

# TODO - 1 for Daniel - TODO Notes
    # create empty database
    # add FEDEFL database
    # Add databases/libraries from LCACommons
    # optional: search for the chemicals we need in this study

########################################################################################################

# This script creates a new process in openLCA
# This code builds on three main existing libraries:
    # 1. netlolca
    # 2. olca_schema
    # 3. olca_ipc

# Import libraries
import json
import pandas as pd
import olca_schema as olca
import olca_ipc
import netlolca
from netlolca import NetlOlca
import logging
import re
import uuid
import datetime
from typing import List, Optional, Tuple, Union


logger = logging.getLogger(__name__)


########################################################
# Define main function
########################################################
# Arguments:
    # a. Dataframe with process data
    # b. Process name
    # c. Process description

def create_new_process(client, df, process_name, process_description):
    # client is initialized before running this function
    # client = olca_ipc.Client()
    # 1. Read dataframe and review its structure
    df = read_dataframe(df)

    # 2. Process metadata
    # Process name
    """
    the process name should be provided by the user before calling this function
    TODO: check if openlca has a limit on the number of characters for the name
    """
    name = process_name
    # Process description
    """        
    the process description should be provided by the user before calling this function
    TODO: check if openlca has a limit on the number of characters for the description
    """
    description = process_description
    
    # 3. Create empty process
    process = create_empty_process(client, name, description)
    # TODO: use function from netlolca to create a new process

    # 4. Create exchange for reference product first
    exchanges = []

    # Loop through the dataframe, find reference product, and create exchange for it
    
    for index, row in df.iterrows():
    #TODO: modify function to read flow type - if flow type is elementary, then skip
        product = row['Flow_Name']
        if row['Reference_Product'] == True:
            exchange = create_ref_product_exchange(client, product, row['LCA_Amount'], row['LCA_Unit'], row['Is_Input'], row['Reference_Product'])
            exchanges.append(exchange)
        else:
            # TODO: Handle non-reference product flows here
            # For now, we skip non-reference product flows
            print(f"Skipping non-reference product flow: {product}")
            continue

    # 5. Create exchange for elementary flows
    # TODO: write function to create exchange for elementary flows
    #       the function should be able to read the uuid for each elementary flow from the dataframe and use it to create the exchange

    # 6. Create exchange for product and waste flows
    # TODO: write function that loops through the dataframe
    #       for each row, the function prompts the user to enter keyword
    #       the function uses the keyword, runs a query, and finds matching flows
    #       the function then prompts the user to select a flow from the list of matching flows
    #       the user should then select a process associated with the selected flow
    #       the function then creates an exchange using the selected flow, process, and unit 

    # 5. Create process
    process.exchanges = exchanges

    # 6. Save process to openLCA
    created_process = client.client.put(process)
    print(f"Successfully created process: {process_name}")
    print(f"Process saved successfully to openLCA database!")    
    return created_process

########################################################
# Define helper functions
########################################################

# Read dataframe and review its structure
########################################################

def read_dataframe(df):
    # Read dataframe - handle both file path and DataFrame object
    if isinstance(df, str):
        # If df is a string (file path), read the CSV file
        df = pd.read_csv(df)
    elif isinstance(df, pd.DataFrame):
        # If df is already a DataFrame, use it directly
        pass
    else:
        raise TypeError("df must be either a file path (string) or a pandas DataFrame")
    
    # Validate structure
    # The dataframe should have the following columns:
    # Flow_Name, LCA_Amount, LCA_Unit, Is_Input, Reference_Product, Flow_Type

    required_columns = ['Flow_Name', 'LCA_Amount', 'LCA_Unit', 'Is_Input', 'Reference_Product', 'Flow_Type']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"The dataframe must have the following columns: {required_columns}")
    return df

# Create empty process TODO: (CAN BE REPLACED WITH NETLOLCA'S FUNCTION)
########################################################
def create_empty_process(client, process_name, process_description):
    process_id = generate_id("process")
    process = olca.Process(
        id=process_id,
        name=process_name,
        description=process_description,
        process_type=olca.ProcessType.UNIT_PROCESS,
        version="1.0.0",
        last_change=datetime.datetime.now().isoformat()
    )
    
    return process

# Generate ID
########################################################

def generate_id(prefix: str = "entity") -> str:
    """
    Generate a unique ID for openLCA entities.
    
    Args:
        prefix (str): Prefix for the ID (e.g., 'process', 'flow', 'unit') - Note: prefix is ignored to comply with database VARCHAR(36) limit
    
    Returns:
        str: Unique ID (36-character UUID string)
    """
    return str(uuid.uuid4())

# Create exchanges 
########################################################
# TODO: TEST THIS FUNCTION AND TEST IF THE NETLOLCA FUNCTION CAN ALSO CREATE A REFERENCE PRODUCT EXCHANGE

# Creating an exchange requires defining its main attributes 
# The main attributes (from reviewing individual JSON files) are defined in the following order:
    # @type --> flow
    # @id --> to be generated for the reference product since it's a new flow
    # name --> the name of the flow from the dataframe
    # description --> can be left blank
    # version --> default set to "00.00.000"
    # flowType --> there are three main flow types: ELEMENTARY_FLOW, PRODUCT_FLOW, WASTE_FLOW
        # reference product flow is a PRODUCT_FLOW
    # isInfrastructureFlow --> set to False
    # flowProperties
        # 0
            # @type --> default set to "flowPropertyFactor"
            # isRefFlowProperty --> set to True
        # flowProperty
            # @type --> default set to "FlowProperty"
            # @id --> same as the flow id generated above
            # name --> depends on unit -- e.g., if unit is kg --> this would be set to "Mass" 
            # category --> default set to "Technical flow properties"
            # refUnit --> taken from df (e.g., kg)
     
# Helper function to find flow property for a unit
def find_flow_property_for_unit(client, unit_obj):
    """
    Find a flow property that contains the given unit.
    
    Args:
        client: NetlOlca client object
        unit_obj: The unit object to find a flow property for
    
    Returns:
        olca.FlowProperty: A flow property containing this unit or None if not found
    """
    try:
        # Get all flow properties using NetlOlca's method
        flow_properties = client.get_all(olca.FlowProperty)
        
        # Search through flow properties to find one that has the unit in its unit group
        for flow_property in flow_properties:
            if hasattr(flow_property, 'unit_group') and flow_property.unit_group:
                # Get the unit group
                if hasattr(flow_property.unit_group, 'id'):
                    unit_group = client.client.get(olca.UnitGroup, flow_property.unit_group.id)
                    if hasattr(unit_group, 'units') and unit_group.units:
                        for unit in unit_group.units:
                            if hasattr(unit, 'id') and hasattr(unit_obj, 'id'):
                                if unit.id == unit_obj.id:
                                    return flow_property
                            elif hasattr(unit, 'name') and hasattr(unit_obj, 'name'):
                                if unit.name == unit_obj.name:
                                    return flow_property
                                    
    except Exception as e:
        print(f"Error finding flow property for unit: {e}")
        
    return None

# Helper function to find unit by name
# We need this function because sometimes openLCA does not take the unit from the dataframe if it is not an exact match
# This function will search for a matching unit in the openLCA database and use it for the exchange
def find_unit_by_name(client, unit_name: str):
    """
    Find a unit by name in the openLCA database using NetlOlca methods.
    
    Args:
        client: NetlOlca client object
        unit_name (str): The unit name to search for
    
    Returns:
        olca.Unit: The first matching unit or None if not found
    """
    try:
        # Get all unit groups using NetlOlca's method
        unit_groups = client.get_all(olca.UnitGroup)
        unit_name_lower = unit_name.lower()
        
        # Search through all unit groups to find the unit
        for unit_group in unit_groups:
            if hasattr(unit_group, 'units') and unit_group.units:
                for unit in unit_group.units:
                    if hasattr(unit, 'name') and unit.name:
                        if unit.name.lower() == unit_name_lower:
                            return unit
                        
        # If exact match not found, try partial match
        for unit_group in unit_groups:
            if hasattr(unit_group, 'units') and unit_group.units:
                for unit in unit_group.units:
                    if hasattr(unit, 'name') and unit.name:
                        if unit_name_lower in unit.name.lower():
                            return unit
                            
    except Exception as e:
        print(f"Error finding unit '{unit_name}': {e}")
        
    return None

# TODO: add option to create reference product from existing technosphere flow
def create_ref_product_exchange(client, flowName, amount, unit, isInput, isRef):
    # Convert unit string to unit object if needed
    if isinstance(unit, str):
        unit_obj = find_unit_by_name(client, unit)
        if unit_obj is None:
            raise ValueError(f"Could not find unit '{unit}' in the openLCA database")
    else:
        unit_obj = unit
    
    # Find a flow property that contains this unit
    flow_property = find_flow_property_for_unit(client, unit_obj)
    if flow_property is None:
        raise ValueError(f"Could not find a flow property for unit '{unit_obj.name}' in the openLCA database")
    
    # Create reference to the flow property
    flow_property_ref = olca.Ref(
        id=flow_property.id,
        name=flow_property.name
    )
    
    # Create flow property factor with proper reference
    ex_flow_property_factor = olca.FlowPropertyFactor(
        conversion_factor = 1.0,
        is_ref_flow_property = True,
        flow_property = flow_property_ref
    )
    
    #Create flow with proper flow properties
    ex_flow = olca.Flow(
        id = generate_id(),
        name = flowName,
        description = f"Product flow for {flowName}",
        flow_type = olca.FlowType.PRODUCT_FLOW,
        flow_properties = [ex_flow_property_factor]
    )
    
    # Save the flow to the database first
    saved_flow = client.client.put(ex_flow)
    print(f"Created flow: {saved_flow.name} with ID: {saved_flow.id}")
    
    # Create a flow reference for the exchange
    flow_ref = olca.Ref(
        id=saved_flow.id,
        name=saved_flow.name
    )
    
    # Use the saved flow reference to create the exchange
    exchange = olca.Exchange(
        flow = flow_ref,
        flow_property = ex_flow_property_factor,
        unit = unit_obj,
        amount = amount,
        is_input = isInput,
        is_quantitative_reference = isRef
    )
    
    return exchange

# search for flows by keyword
########################################################
def search_Flows_by_keywords(client, keywords: str, flow_type: Optional[olca.FlowType] = None):
    """
    Search for processes by keywords using netlolca's existing functions.
    Focuses primarily on process names with smart matching and sorting.
    Optionally filters by flow type in process exchanges.
    
    Args:
        netl_client (NetlOlca): The netlolca client instance
        keywords (str): Keywords to search for
        flow_type (Optional[olca.FlowType]): Flow type to filter by (e.g., olca.FlowType.PRODUCT_FLOW, 
                                           olca.FlowType.ELEMENTARY_FLOW, olca.FlowType.WASTE_FLOW)
    
    Returns:
        List[olca.Process]: List of processes matching the keywords and flow type, sorted by relevance
    """
    try:
        print (f"Searching for flows containing '{keywords}'...")
        #modify keywords for better matching
        # Using re.escape to handle special regex characters in keywords
        escaped_keywords = re.escape(keywords)
        pattern = re.compile(f".*{escaped_keywords}.*", re.IGNORECASE)

        # Get all flow descriptors
        flow_descriptors = client.get_descriptors(olca.Flow)
        if not flow_descriptors:
            print("No flows found in database")
            return []

        matching_descriptors = []
        for descriptor in flow_descriptors:
            if pattern.search(descriptor.name.lower()):
                matching_descriptors.append(descriptor)

        if not matching_descriptors:
            print(f"No flows found matching '{keywords}'")
            return []
        
        print(f"Found {len(matching_descriptors)} flows matching '{keywords}'")

        # Get full flow objects and filter by type if specified
        matching_flows = []
        for descriptor in matching_descriptors:
            try:
                flow = client.query(olca.Flow, descriptor.id)
                if flow:
                    # Filter by flow type if specified
                    if flow_type is None or flow.flow_type == flow_type:
                        matching_flows.append(flow)
            except Exception as e:
                logger.warning(f"Could not retrieve flow {descriptor.id}: {e}")
                continue
        
        if flow_type:
            print(f"Filtered to {len(matching_flows)} {flow_type.name} flows")
        
        return matching_flows
        
    except Exception as e:
        logger.warning(f"Could not search for flows: {e}")
        return []

################################################################
#Interface Code
################################################################

def show_flow_process_selection_menu(flow_name: str, matching_items: List, search_keywords: str = None) -> Optional[Union[olca.Process, str, Tuple[str, olca.Flow]]]:
    """
    Show a menu for user to select a process or flow for a specific flow.
    
    Args:
        flow_name (str): Name of the flow being processed
        matching_items (List): List of matching processes or flows
        search_keywords (str): Keywords used for search (for display purposes)
    
    Returns:
        Optional[Union[olca.Process, str, Tuple[str, olca.Flow]]]: Selected process, flow type string, or existing flow tuple
    """
    
    if not matching_items:
        print(f"No existing flows found matching '{search_keywords}'")
        print("Options:")
        print("1. Create new elementary flow")
        print("2. Create new product flow")
        print("3. Create new waste flow")
        print("0. Skip this flow")
        
        while True:
            try:
                choice = input(f"\nSelect option for '{flow_name}' (0-3): ").strip()
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
                elif choice_num == 3:
                    print(f"Will create new waste flow: {flow_name}")
                    return "WASTE_FLOW"
                else:
                    print("Please enter a number between 0 and 2")
                    
            except ValueError:
                print("Please enter a valid number")
            except KeyboardInterrupt:
                print("\nSelection cancelled.")
                return None
    
    print(f"Found {len(matching_items)} matching '{search_keywords}':")
    # Display flows
    for i, flow in enumerate(matching_items, 1):
        print(f"\n{i}. {flow.name} (ID: {flow.id})")
        if hasattr(flow, 'description') and flow.description:
            print(f"   Description: {flow.description}")
        print(f"   Type: {getattr(flow, 'flow_type', 'Unknown')}")
    
    print(f"\n{len(matching_items) + 1}. Create new elementary flow")
    print(f"{len(matching_items) + 2}. Create new product flow")
    print(f"{len(matching_items) + 3}. Create new waste flow")
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


# Get User Search Choice
#########################################################################

def get_user_search_choice(flow_name: str) -> tuple:
    """
    Ask user to choose search type and keywords for a specific flow.
    
    Args:
        flow_name (str): Name of the flow being processed
    
    Returns:
        tuple: (search_type, search_keywords) where search_type is 'process', 'flow', or 'skip'
    """
    print(f"\nSearch Options for Flow: {flow_name}")
    print("Choose search strategy:")
    print("   1. Search for product flows")
    print("   2. Search in elementary flows")
    print("   3. Search in waste flows")
    print("   3. Skip this flow")

    
    while True:
        try:
            choice = input(f"\nChoose search type for '{flow_name}' (1=product, 2=elementary, 3=waste, 4=skip): ").strip()
            
            if choice == '1':
                search_type = 'product flow'
                break
            elif choice == '2':
                search_type = 'elementary flow'
                break
            elif choice == '3':
                search_type = 'waste flow'
                break
            elif choice == '4':
                search_type = 'skip'
                break
            else:
                print("Please enter 1, 2, 3, or 4")
                continue
                
        except KeyboardInterrupt:
            return ('skip', None)
    
    # Get search keywords
    print(f"\nEnter search keywords for {search_type.upper()} search:")
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


########################################################
# Flow Properties and Units Functions
########################################################

def get_flow_properties(client, flow_uuid):
    """
    Get flow properties for a given flow.

    Args:
        client: OpenLCA client instance
        flow_uuid (str): Universally unique identifier (UUID) of the flow

    Returns:
        list: List of flow property dictionaries with 'id', 'name', and 'unit_group' keys,
              or empty list if flow not found or no properties exist.
    """
    try:
        flow = client.query(olca.Flow, flow_uuid)
        if not flow or not flow.flow_properties:
            return []
        
        properties = []
        for fp in flow.flow_properties:
            if fp.flow_property and fp.flow_property.ref:
                prop_obj = client.query(olca.FlowProperty, fp.flow_property.ref.id)
                if prop_obj:
                    prop_data = {
                        'id': prop_obj.id,
                        'name': prop_obj.name,
                        'unit_group': prop_obj.unit_group.ref.id if prop_obj.unit_group and prop_obj.unit_group.ref else None
                    }
                    properties.append(prop_data)
        return properties
    except Exception as e:
        logger.warning(f"Could not retrieve flow properties for {flow_uuid}: {e}")
        return []


def get_flow_units(client, flow_uuid, flow_property_uuid=None):
    """
    Get available units for a flow's properties.

    Args:
        client: OpenLCA client instance
        flow_uuid (str): Universally unique identifier (UUID) of the flow
        flow_property_uuid (str, optional): Specific flow property UUID to get units for. 
                                          If None, returns units for the first/reference flow property.

    Returns:
        list: List of unit dictionaries with 'id', 'name', and 'symbol' keys,
              or empty list if no units found.
    """
    try:
        flow = client.query(olca.Flow, flow_uuid)
        if not flow or not flow.flow_properties:
            return []
        
        # Find the target flow property
        target_fp = None
        if flow_property_uuid:
            # Look for specific flow property
            for fp in flow.flow_properties:
                if fp.flow_property and fp.flow_property.ref and fp.flow_property.ref.id == flow_property_uuid:
                    target_fp = fp
                    break
        else:
            # Use reference flow property (first one with reference_flow_property=True, or first one)
            for fp in flow.flow_properties:
                if fp.reference_flow_property:
                    target_fp = fp
                    break
            if not target_fp and flow.flow_properties:
                target_fp = flow.flow_properties[0]
        
        if not target_fp or not target_fp.flow_property or not target_fp.flow_property.ref:
            return []
        
        # Get the flow property and its unit group
        flow_prop = client.query(olca.FlowProperty, target_fp.flow_property.ref.id)
        if not flow_prop or not flow_prop.unit_group or not flow_prop.unit_group.ref:
            return []
        
        unit_group = client.query(olca.UnitGroup, flow_prop.unit_group.ref.id)
        if not unit_group or not unit_group.units:
            return []
        
        # Extract unit information
        units = []
        for unit in unit_group.units:
            unit_data = {
                'id': unit.id,
                'name': unit.name,
                'symbol': getattr(unit, 'symbol', unit.name)
            }
            units.append(unit_data)
        
        return units
    except Exception as e:
        logger.warning(f"Could not retrieve units for flow {flow_uuid}: {e}")
        return []


def select_flow_property_and_unit(client, flow, flow_name):
    """
    Interactive function to let user select flow property and unit for a given flow.
    
    Args:
        client: OpenLCA client instance
        flow: The flow object
        flow_name (str): Name of the flow for display purposes
        
    Returns:
        tuple: (flow_property_obj, unit_obj) or (None, None) if cancelled
    """
    # Get flow properties
    properties = get_flow_properties(client, flow.id)
    if not properties:
        print(f"❌ No flow properties found for flow: {flow_name}")
        return None, None
    
    # If only one property, use it
    if len(properties) == 1:
        selected_property = properties[0]
        print(f"✅ Using flow property: {selected_property['name']}")
    else:
        # Let user select flow property
        print(f"\n📊 Select flow property for '{flow_name}':")
        for i, prop in enumerate(properties, 1):
            print(f"   {i}. {prop['name']} (ID: {prop['id']})")
        
        while True:
            try:
                choice = input(f"Select property (1-{len(properties)}): ").strip()
                choice_num = int(choice)
                if 1 <= choice_num <= len(properties):
                    selected_property = properties[choice_num - 1]
                    break
                else:
                    print(f"Please enter a number between 1 and {len(properties)}")
            except ValueError:
                print("Please enter a valid number")
            except KeyboardInterrupt:
                return None, None
    
    # Get units for selected property
    units = get_flow_units(client, flow.id, selected_property['id'])
    if not units:
        print(f"No units found for property: {selected_property['name']}")
        return None, None
    
    # If only one unit, use it
    if len(units) == 1:
        selected_unit = units[0]
        print(f"Using unit: {selected_unit['name']} ({selected_unit['symbol']})")
    else:
        # Let user select unit
        print(f"\n📏 Select unit for property '{selected_property['name']}':")
        for i, unit in enumerate(units, 1):
            print(f"   {i}. {unit['name']} ({unit['symbol']})")
        
        while True:
            try:
                choice = input(f"Select unit (1-{len(units)}): ").strip()
                choice_num = int(choice)
                if 1 <= choice_num <= len(units):
                    selected_unit = units[choice_num - 1]
                    break
                else:
                    print(f"Please enter a number between 1 and {len(units)}")
            except ValueError:
                print("Please enter a valid number")
            except KeyboardInterrupt:
                return None, None
    
    # Get the actual objects
    try:
        flow_property_obj = client.query(olca.FlowProperty, selected_property['id'])
        unit_obj = client.query(olca.Unit, selected_unit['id'])
        return flow_property_obj, unit_obj
    except Exception as e:
        logger.error(f"Failed to retrieve flow property or unit objects: {e}")
        return None, None