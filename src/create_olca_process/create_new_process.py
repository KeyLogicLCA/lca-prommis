# This script creates a new process in openLCA
# This code builds on three main existing libraries:
    # 1. netlolca
    # 2. olca_schema
    # 3. olca_ipc

# Import libraries
import json
import pandas as pd
import olca_schema
import olca_ipc
import netlolca

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

    # 4. Create exchanges
    exchanges = []

    # Loop through the dataframe and create and exchange for each flow
    for index, row in df.iterrows():
        product = row['Flow_Name']
        if row['Reference_Product'] == True:
            exchange = create_ref_product_exchange(client, process_name, product, row['LCA_Amount'], row['LCA_Unit'], row['Is_Input'], row['Reference_Product'], row['Flow_Type'])
        else:


        exchanges.append(exchange)
    # 5. Create process
    process.exchanges = exchanges

    # 6. Save process to openLCA
    created_process = client.put(process)
    print(f"Successfully created process: {process_name} (ID: {process_id})")
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

# Create empty process
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
     
def create_ref_product_exchange(client, df):
    # Create reference product exchange
    ex_flow_property_factor = olca.FlowPropertyFactor(
        conversion_factor = 1.0,
        is_ref_flow_property = df['Reference_Product']
    )
    #Create flow 
    ex_flow = olca.Flow(
        id = generate_id(),
        name = df['Flow_Name'],
        description = df['Flow_Description'],
        flow_type = client.FlowType.PRODUCT_FLOW,
        flow_properties = [ex_flow_property_factor]
    )
    # Use this flow to create the exchange
    exchange = olca.Exchange(
        flow = ex_flow,
        flow_property = ex_flow_property_factor,
        unit = df['LCA_Unit'],
        amount = df['LCA_Amount'],
        is_input = df['Is_Input'],
        is_quantitative_reference = df['Reference_Product']
    )

########################################################