########################################################################################################
# TODO's in this script:
# Writing/editing functions
    # use function from netlolca to create a new process and test new function
    # Develop function to get flow property of the flow selected by the user
    # Develop function to get the available units for the flow selected by the user
    # Use function from netlolca library to create exchange for the selected flow 
    # Create exchange for the reference product flow using netlolca library

# Testing/debugging
    # Test (and debug if needed): create_ref_product_exchange                               --> DONE
    # Test (and debug if needed): search_Flows_by_keywords
    # Test (and debug if needed): show_flow_process_selection_menu
    # Test (and debug if needed): get_user_search_choice
    # Test (and debug if needed): create_new_process
    # Test (and debug if needed): get_flow_properties
    # Test (and debug if needed): get_flow_units
    # Test (and debug if needed): select_flow_property_and_unit

# Other
    # check if openlca has a limit on the number of characters for the name
    # check if openlca has a limit on the number of characters for the description 
    # for elementary flows - Daniel will add uuids for each flow (FEDEFL)
    # create openLCA database with all the available processes/providers
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

    # 4. Create exchanges
    exchanges = []

    # Loop through the dataframe and create and exchange for each flow
    # Federal elementary flow - existing python package
    # TODO: write code that gets databases from LCACommons for chemicals
        #   check LCA Commons API
    
    for index, row in df.iterrows():
        product = row['Flow_Name']
        if row['Reference_Product'] == True:
            exchange = create_ref_product_exchange(client, product, row['LCA_Amount'], row['LCA_Unit'], row['Is_Input'], row['Reference_Product'])
        else:
            # show flow process selection menu to:
            # a. ask user to define the flow type: elementary, product, waste
            # b. ask user to enter search keyword
            search_type, search_keywords = get_user_search_choice(row['Flow_Name'])

            if search_type == "skip":
                print(f"Skipping flow: {row['Flow_Name']}")
                continue

            #search based on user's choice
            if search_type == 'product flow':
                print(f"Searching for product flows containing '{search_keywords}'...")
                matching_items = search_Flows_by_keywords(client, search_keywords, olca.FlowType.PRODUCT_FLOW)
                print(f"Found {len(matching_items)} matching processes")

            elif search_type == 'elementary flow':
                print(f"Searching for flows containing '{search_keywords}'...")
                matching_items = search_Flows_by_keywords(client, search_keywords, olca.FlowType.ELEMENTARY_FLOW)
                print(f"Found {len(matching_items)} matching flows")

            else:
                print(f"Searching for waste flows containing '{search_keywords}'...")
                matching_items = search_Flows_by_keywords(client, search_keywords, olca.FlowType.WASTE_FLOW)
                print(f"Found {len(matching_items)} matching flows")


            # Once given the list of matching flows, ask user to select what flow to use
            selection = show_flow_process_selection_menu(row['Flow_Name'], matching_items, search_keywords)



            # TODO: 
            # Once the user selects a flow ('selection' variable)
            # compile the flow, and flow property of that flow
            # get available units for said flow and ask user to select a unit
            # create exchange using selected flow, flow property, and unit

            exchange = olca.Exchange(
                flow = flow,
                flow_property = flow_property,
                unit = unit,
                amount = df['LCA_Amount'],
                is_input = df['Is_Input'],
                is_quantitative_reference = df['Reference_Product']
            )

        exchanges.append(exchange)

    # 5. Create process
    process.exchanges = exchanges

    # 6. Save process to openLCA
    created_process = client.put(process)
    print(f"Successfully created process: {process_name}")
    print(f"Process saved successfully to openLCA database!")    
    return created_process

########################################################
# Define helper functions
########################################################

# Read dataframe and review its structure
########################################################

def read_dataframe(df):
    # Read dataframe
    df = pd.read_csv(df)
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
        last_change=datetime.now().isoformat()
    )

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
     
# TODO: add option to create reference product from existing technosphere flow
def create_ref_product_exchange(client, flowName, amount, unit, isInput, isRef):
    # Create reference product exchange
    ex_flow_property_factor = olca.FlowPropertyFactor(
        conversion_factor = 1.0,
        is_ref_flow_property = True
    )
    #Create flow 
    ex_flow = olca.Flow(
        id = generate_id(),
        name = flowName,
        description = "",
        flow_type = client.FlowType.PRODUCT_FLOW,
        flow_properties = [ex_flow_property_factor]
    )
    # Use this flow to create the exchange
    exchange = olca.Exchange(
        flow = ex_flow,
        flow_property = ex_flow_property_factor,
        unit = unit,
        amount = amount,
        is_input = isInput,
        is_quantitative_reference = isRef
    )

# search for product flows by keyword
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
        print(f"❌ No units found for property: {selected_property['name']}")
        return None, None
    
    # If only one unit, use it
    if len(units) == 1:
        selected_unit = units[0]
        print(f"✅ Using unit: {selected_unit['name']} ({selected_unit['symbol']})")
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