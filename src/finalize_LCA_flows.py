import pandas as pd
import numpy as np
from typing import Union, List, Optional
import fedelemflowlist as ffl


"""
LCA Flows Finalization and Processing Module

This module provides comprehensive functionality for processing, merging, and finalizing
Life Cycle Assessment (LCA) flow data. It is designed to work with LCA DataFrames
that contain flow information from various sources and convert them into standardized,
functional unit-based formats suitable for openLCA and other LCA software.

Key Features:
- Merge multiple flows into consolidated categories (e.g., REO feeds, waste streams)
- Convert flows to functional units based on reference products
- Map flow types to openLCA categories and contexts
- Generate UUIDs for elementary flows using the Federal LCA Commons flow list
- Handle duplicate flows by merging and summing amounts
- Provide comprehensive validation and summary statistics

Main Functions:
- main(): Orchestrates the complete workflow for REO processing
- finalize_df(): Converts flows to functional units and standardizes format
- merge_flows(): Combines flows based on source or category
- convert_to_functional_unit(): Normalizes flows using reference flow scaling
- get_uuid(): Retrieves UUIDs for elementary flows
- merge_duplicate_flows(): Consolidates duplicate flow entries

Usage:
    from finalize_LCA_flows import main
    
    # Process REO flows with default parameters
    finalized_df = main()
    
    # Customize reference flow and water type
    finalized_df = main(
        reference_flow='Custom Product',
        reference_source='Custom Source',
        water_type='treated water'
    )

Dependencies:
- pandas: Data manipulation and analysis
- numpy: Numerical operations
- fedelemflowlist: Federal LCA Commons flow list access
- typing: Type hints for function parameters and returns
"""


# Global dictionary for mapping categories to openLCA categories
category_mapping = {
    'water': 'Elementary flows',
    'emissions to air': 'Elementary flows',
    'emissions to water': 'Elementary flows',
    'emissions to ground': 'Elementary flows',
    'emission to air': 'Elementary flows',
    'emission to water': 'Elementary flows',
    'emission to ground': 'Elementary flows',
    'resource: water': 'Elementary flows',
    'resource: air': 'Elementary flows',
    'resource: ground': 'Elementary flows',
    'resource: biotic': 'Elementary flows',
    'chemicals': 'Technosphere flows',
    'solid input': 'Technosphere flows',
    'solid output': 'Technosphere flows',
    'electricity': 'Technosphere flows',
    'heat': 'Technosphere flows',
    'wastewater': 'Waste flows',
    'solid waste': 'Waste flows',
}

# Global dictionary for mapping categories to contexts in openLCA for elementary flows
context_mapping = {
    'water': 'resource/water',
    'emissions to air': 'emission/air',
    'emissions to water': 'emission/water',
    'emissions to ground': 'emission/ground',
    'emission to air': 'emission/air',
    'emission to water': 'emission/water',
    'emission to ground': 'emission/ground',
    'resource: water': 'resource/water',
    'resource: air': 'resource/air',
    'resource: ground': 'resource/ground',
    'resource: biotic': 'resource/biotic',
}

# Example usage
def main(reference_flow: str = '99.85% REO Product', reference_source: str = 'Roaster Product', water_type: str = 'raw fresh water'):
    """
    Main function to demonstrate the complete LCA flows processing workflow.
    
    This function orchestrates the entire process of merging, finalizing, and converting
    LCA flows to functional units. It performs the following steps:
    1. Loads the converted LCA DataFrame
    2. Merges REO feed flows into a single 374 ppm REO Feed
    3. Merges roaster product flows into a 99.85% REO Product
    4. Consolidates wastewater and solid waste flows
    5. Finalizes the DataFrame with proper categorization and UUIDs
    6. Saves the result and provides a summary
    
    Parameters
    ----------
    reference_flow : str, optional
        Name of the reference flow for functional unit conversion (default: '99.85% REO Product')
    reference_source : str, optional
        Source of the reference flow (default: 'Roaster Product')
    water_type : str, optional
        Type of water to specify in descriptions (default: 'raw fresh water')
    
    Returns
    -------
    pandas.DataFrame
        The finalized LCA DataFrame with all flows converted to functional units
        
    Notes
    -----
    The 374 ppm REO feed concentration is calculated from the flowsheet, though the
    original study used 357 ppm. Some wastewater streams are organic waste but are
    treated as wastewater for consistency.
    """
    df = pd.read_csv('output/lca_df_converted.csv')
    
    # Run the merge_flows function for the feed
    REO_list = [
        "Yttrium Oxide",
        "Lanthanum Oxide",
        "Cerium Oxide",
        "Praseodymium Oxide",
        "Neodymium Oxide",
        "Samarium Oxide",
        "Gadolinium Oxide",
        "Dysprosium Oxide",
    ]
    df = merge_flows(df, merge_source='Solid Feed', new_flow_name='374 ppm REO Feed', value_2_merge=REO_list)
    # This 374 ppm value is directly calculated from the flowsheet. The original study actually used 357 ppm as the feed concentration.
    
    # Run the merge_flows function for the product
    df = merge_flows(df, merge_source='Roaster Product', new_flow_name='99.85% REO Product')
    
    # Run the merge_flows function for the liquid waste flows
    df = merge_flows(df, merge_source='Wastewater', new_flow_name='Wastewater', merge_column='Category') 
    # Note: some of these streams are organic waste, but they're treated as wastewater

    # Run the merge_flows function for the solid waste flows
    df = merge_flows(df, merge_source='Solid Waste', new_flow_name='Solid Waste', merge_column='Category') 
    
    # Run the finalize_df function
    try:
        finalized_df = finalize_df(
            df=df,
            reference_flow=reference_flow,
            reference_source=reference_source,
            water_type=water_type
        )
        
        # Get summary
        summary = get_finalize_summary(finalized_df)
        print("Summary:")
        for key, value in summary.items():
            if key != 'flow_type_breakdown':
                print(f"  {key}: {value}")
        
        print("\nFlow Type Breakdown:")
        for flow_type, count in summary['flow_type_breakdown'].items():
            print(f"  {flow_type}: {count}")
            
    except Exception as e:
        print(f"Error during finalization: {e}")
    
    finalized_df.to_csv('output/lca_df_finalized.csv', index=False)
    return finalized_df


def finalize_df(df: pd.DataFrame, 
                reference_flow: str, 
                reference_source: str,
                water_type: str = 'raw fresh water') -> pd.DataFrame:
    """
    Finalize the LCA DataFrame by converting to functional units and creating a standardized format.
    
    This function takes a DataFrame after merge_flows operations and:
    1. Converts all flows to functional units based on the reference flow
    2. Creates a new DataFrame with standardized columns
    3. Merges duplicate flows based on flow name, type, and input/output status
    
    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame with LCA flows (after merge_flows operations)
    reference_flow : str
        Name of the reference flow for functional unit conversion
    reference_source : str
        Source of the reference flow for functional unit conversion
    
    Returns
    -------
    pandas.DataFrame
        Finalized DataFrame with columns: ['Flow_Name', 'LCA_Amount', 'LCA_Unit', 
        'Is_Input', 'Reference_Product', 'Flow_Type', 'Category', 'Context', 'UUID', 'Description']
    """
    # Step 1: Convert to functional units
    df_functional = convert_to_functional_unit(df, reference_flow, reference_source)
    
    # Step 2: Create new DataFrame with required columns
    finalized_data = []
    
    for idx, row in df_functional.iterrows():
        # Required columns
        flow_name = row['Flow']
        lca_amount = row['LCA Amount']
        lca_unit = row['LCA Unit']
        
        # Optional columns
        is_input = row['In/Out'].lower() == 'in'
        reference_product = (row['Flow'] == reference_flow and 
                           row['Source'] == reference_source)
        flow_type = row['Category']
        # If it is water, we can mention the water type. Otherwise, the description is blank.
        description = ''
        if flow_type == 'Water':
            try:
                description = f'{water_type}'
            except:
                print(f'Error getting water type {water_type}: {e}')
                description = ''
        
        # Map the flow type to the openLCA category if it exists in the category_mapping dictionary
        lower_flow_type = flow_type.lower()
        if lower_flow_type in category_mapping.keys():
            category = category_mapping[lower_flow_type]
        else:
            category = flow_type
        
        # Can only generate these for elementary flows. Otherwise, they will be left empty strings.
        context = ''
        uuid = ''
        if category == 'Elementary flows':
            # So we only define elem_df once:
            try:
                elem_df = elem_df
            except:
                elem_df = ffl.get_flows()
            
            try:
                lower_flow_type = flow_type.lower()
                context = context_mapping[lower_flow_type]
                uuid = get_uuid(flow_name, context, elem_df)
            
            # We won't be able to generate a UUID if the context cannot be generated
            except KeyError:
                print(f'{flow_type} not found in context_mapping. Cannot generate context or UUID for {flow_name}.')
            
            except Exception as e:
                print(f'Error generating UUID for {flow_name}: {e}')
        
        # Convert Heat flows to Natural Gas flows
        if flow_type == "Heat" or flow_name == "Heat":
            flow_name = "Natural Gas"
            flow_type = "Heat"
            lca_unit = "m3"
            lca_amount = lca_amount / 37.3
        
        finalized_data.append({
            'Flow_Name': flow_name,
            'LCA_Amount': lca_amount,
            'LCA_Unit': lca_unit,
            'Is_Input': is_input,
            'Reference_Product': reference_product,
            'Flow_Type': flow_type,
            'Category': category,
            'Context': context,
            'UUID': uuid,
            'Description': description,
        })
    
    # Create the new DataFrame
    finalized_df = pd.DataFrame(finalized_data)
    
    # Step 3: Merge duplicate flows
    finalized_df = merge_duplicate_flows(finalized_df)
    
    return finalized_df


def merge_flows(df: pd.DataFrame, 
                merge_source: str, 
                new_flow_name: str, 
                merge_column: str = 'Source',
                value_1_merge: Union[str, List[str]] = "same",
                value_2_merge: Union[str, List[str]] = "same", 
                LCA_amount_merge: Union[str, List[str]] = "total",
                delete: Union[str, List[str]] = "all") -> pd.DataFrame:
    """
    Merge flows with a specific source into a single flow.
    
    This function combines all flows with the specified source into a new flow,
    with configurable logic for handling values and deletion of original flows.
    
    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame with flows to merge
    merge_source : str
        Source name to match for merging flows
    new_flow_name : str
        Name for the new merged flow
    merge_column : str, optional
        Column name to merge on (default: 'Source')
    value_1_merge : str or list, optional
        Logic for handling Value 1:
        - "same": Keep the value from the first matching flow
        - "total": Sum all Value 1 values from matching flows
        - list: Sum Value 1 values from flows with names in the list
    value_2_merge : str or list, optional
        Logic for handling Value 2 (same options as value_1_merge)
    LCA_amount_merge : str or list, optional
        Logic for handling LCA Amount (default: "total"):
        - "same": Keep the value from the first matching flow
        - "total": Sum all LCA Amount values from matching flows
        - list: Sum LCA Amount values from flows with names in the list
    delete : str or list, optional
        Logic for deleting original flows:
        - "all": Delete all flows with matching source
        - list: Delete flows with names in the list that have matching source
        - other: Don't delete any flows
    
    Returns
    -------
    pandas.DataFrame
        DataFrame with merged flows and deletions applied
    """
    # Create a copy to avoid modifying the original
    df_copy = df.copy()
    
    # Find all flows with matching source
    matching_mask = df_copy[merge_column] == merge_source
    matching_flows = df_copy[matching_mask]
    
    if matching_flows.empty:
        print(f"Warning: No flows found with {merge_column} '{merge_source}'")
        return df_copy
    
    # Get the first matching flow as template
    first_flow = matching_flows.iloc[0]
    insert_index = matching_flows.index[0]
    
    # Create new flow with template data
    new_flow = first_flow.copy()
    new_flow['Flow'] = new_flow_name
    
    # Handle Value 1 merging
    new_flow['Value 1'] = _merge_values(df_copy, merge_source, 'Value 1', value_1_merge, merge_column)
    
    # Handle Value 2 merging
    new_flow['Value 2'] = _merge_values(df_copy, merge_source, 'Value 2', value_2_merge, merge_column)
    
    # Handle LCA Amount merging (if LCA Amount column exists)
    if 'LCA Amount' in df_copy.columns:
        new_flow['LCA Amount'] = _merge_values(df_copy, merge_source, 'LCA Amount', LCA_amount_merge, merge_column)
    
    # Determine which flows to delete
    flows_to_delete = _get_flows_to_delete(df_copy, merge_source, delete, merge_column)
    
    # Delete specified flows
    if flows_to_delete:
        df_copy = df_copy.drop(flows_to_delete)
        # Adjust insert index if the first flow was deleted
        if insert_index in flows_to_delete:
            # Find the new position where the first flow was
            remaining_flows = df_copy[df_copy[merge_column] == merge_source]
            if not remaining_flows.empty:
                insert_index = remaining_flows.index[0]
            else:
                # If no matching flows remain, insert at the end
                insert_index = len(df_copy)
    
    # Insert the new flow at the appropriate position
    df_copy = _insert_flow_at_position(df_copy, new_flow, insert_index)
    
    return df_copy


def convert_to_functional_unit(df: pd.DataFrame, 
                              flow_name: str, 
                              flow_source: str) -> pd.DataFrame:
    """
    Convert all flows to a functional unit based on a reference flow.
    
    This function finds a reference flow and uses its LCA Amount as a scaling factor
    to normalize all other flows in the DataFrame. Both 'Value 1' and 'LCA Amount'
    columns are scaled by the same factor.
    
    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame with flows to convert
    flow_name : str
        Name of the reference flow
    flow_source : str
        Source of the reference flow
    
    Returns
    -------
    pandas.DataFrame
        DataFrame with all Value 1 and LCA Amount values normalized by the reference flow
        
    Raises
    ------
    ValueError
        If the reference flow is not found or has an LCA Amount of 0
        
    Notes
    -----
    If multiple flows match the reference criteria, a warning is printed and the first
    match is used. The scaling factor is applied to both 'Value 1' and 'LCA Amount' columns.
    """
    # Create a copy to avoid modifying the original
    df_copy = df.copy()
    
    # Find the reference flow
    reference_mask = (df_copy['Flow'] == flow_name) & (df_copy['Source'] == flow_source)
    reference_flows = df_copy[reference_mask]
    
    if reference_flows.empty:
        raise ValueError(f"No flow found with name '{flow_name}' and source '{flow_source}'")
    
    if len(reference_flows) > 1:
        print(f"Warning: Multiple flows found with name '{flow_name}' and source '{flow_source}'. Using the first one.")
    
    # Get the scaling factor from the reference flow
    scaling_factor = reference_flows.iloc[0]['LCA Amount']
    
    if scaling_factor == 0:
        raise ValueError(f"Reference flow has Value 1 of 0, cannot use as scaling factor")
    
    # Apply the scaling factor to all Value 1 values
    df_copy['Value 1'] = df_copy['Value 1'] / scaling_factor
    df_copy['LCA Amount'] = df_copy['LCA Amount'] / scaling_factor
    
    print(f"Applied functional unit conversion with scaling factor: {scaling_factor}")
    print(f"Reference flow: {flow_name} from {flow_source}")
    
    return df_copy


def get_uuid(flow_name: str, context: str, elem_df: pd.DataFrame) -> str:
    """
    Retrieve the UUID for a specific flow from the elementary flows database.
    
    This function searches the elementary flows DataFrame to find a matching flow
    based on the flow name and context, then returns the corresponding Flow UUID.
    If no match is found, returns None.
    
    Parameters
    ----------
    flow_name : str
        The name of the flow to search for (must match the "Flowable" column)
    context : str
        The context of the flow (e.g., 'emission/air', 'resource/water')
    elem_df : pandas.DataFrame
        DataFrame containing elementary flows with columns: "Flowable", "Context", "Flow UUID"
    
    Returns
    -------
    str or None
        The Flow UUID if a match is found, None if no match exists
        
    Examples
    --------
    >>> elem_df = ffl.get_flows()
    >>> uuid = get_uuid("Carbon dioxide", "emission/air", elem_df)
    >>> print(uuid)
    '12345678-1234-1234-1234-123456789abc'
    """
    # Look up matching UUID
    match = elem_df[
        (elem_df["Flowable"] == flow_name) &
        (elem_df["Context"] == context)
    ]

    if not match.empty:
        uuid = match.iloc[0]["Flow UUID"]
    else:
        uuid = None  # or "UUID_NOT_FOUND"

    return uuid


def merge_duplicate_flows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge duplicate flows that share the same flow name, flow type, and input/output status.
    
    This function groups flows by Flow_Name, Flow_Type, and Is_Input, then sums their
    LCA_Amount values to create a single consolidated flow entry.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with columns: ['Flow_Name', 'LCA_Amount', 'LCA_Unit', 
        'Is_Input', 'Reference_Product', 'Flow_Type', 'Category', 'Context', 'UUID', 'Description']
    
    Returns
    -------
    pandas.DataFrame
        DataFrame with duplicate flows merged and LCA_Amount values summed
    """
    # Group by the key columns for merging
    group_columns = ['Flow_Name', 'Flow_Type', 'Is_Input']
    
    # Create a list to store the merged data
    merged_data = []
    
    # Group the DataFrame
    grouped = df.groupby(group_columns)
    
    for (flow_name, flow_type, is_input), group in grouped:
        # Sum the LCA_Amount values
        total_amount = group['LCA_Amount'].sum()
        
        # Take the first occurrence for other columns (they should be the same)
        first_row = group.iloc[0]
        
        # Check if any flow in the group is a reference product
        is_reference_product = group['Reference_Product'].any()
        
        # Create the merged row
        merged_row = {
            'Flow_Name': flow_name,
            'LCA_Amount': total_amount,
            'LCA_Unit': first_row['LCA_Unit'],
            'Is_Input': is_input,
            'Reference_Product': is_reference_product,
            'Flow_Type': flow_type,
            'Category': first_row['Category'],
            'Context': first_row['Context'],
            'UUID': first_row['UUID'],
            'Description': first_row['Description'],
        }
        
        merged_data.append(merged_row)
    
    # Create the new DataFrame
    merged_df = pd.DataFrame(merged_data)
    
    # Sort by Flow_Name for better readability
    merged_df = merged_df.sort_values('Flow_Name').reset_index(drop=True)
    
    return merged_df


# Helper Functions

def _merge_values(df: pd.DataFrame, 
                  source: str, 
                  value_column: str, 
                  merge_logic: Union[str, List[str]],
                  merge_column: str = 'Source') -> float:
    """
    Helper function to merge values based on the specified logic.
    
    This function handles the merging of numeric values from flows that share the same
    source (or other specified column). It supports three merging strategies: keeping
    the first value, summing all values, or summing only values from specific flows.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the flows
    source : str
        Source value to match in the merge_column
    value_column : str
        Column name containing the values to merge ('Value 1', 'Value 2', or 'LCA Amount')
    merge_logic : str or list
        Logic for merging values:
        - "same": Keep the value from the first matching flow
        - "total": Sum all values from matching flows
        - list: Sum values from flows with names in the list
    merge_column : str, optional
        Column name to use for matching flows (default: 'Source')
    
    Returns
    -------
    float
        Merged value according to the specified logic
        
    Notes
    -----
    If merge_logic is a list, only flows with names in that list are included in the sum.
    If merge_logic is not recognized, defaults to "same" behavior.
    """
    matching_flows = df[df[merge_column] == source]
    
    if merge_logic == "same":
        # Return the value from the first matching flow
        return matching_flows.iloc[0][value_column]
    
    elif merge_logic == "total":
        # Sum all values from matching flows
        return matching_flows[value_column].sum()
    
    elif isinstance(merge_logic, list):
        # Sum values from flows with names in the list
        flows_to_sum = matching_flows[matching_flows['Flow'].isin(merge_logic)]
        return flows_to_sum[value_column].sum()
    
    else:
        # Default to "same" behavior
        return matching_flows.iloc[0][value_column]


def _get_flows_to_delete(df: pd.DataFrame, 
                        source: str, 
                        delete_logic: Union[str, List[str]],
                        merge_column: str = 'Source') -> List[int]:
    """
    Helper function to determine which flows should be deleted after merging.
    
    This function identifies the indices of flows that should be removed from the
    DataFrame based on the deletion logic. It supports deleting all matching flows
    or only specific flows by name.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the flows
    source : str
        Source value to match in the merge_column
    delete_logic : str or list
        Logic for determining deletions:
        - "all": Delete all flows with matching source
        - list: Delete flows with names in the list that have matching source
        - other: Don't delete any flows
    merge_column : str, optional
        Column name to use for matching flows (default: 'Source')
    
    Returns
    -------
    list
        List of DataFrame indices to delete
        
    Notes
    -----
    If delete_logic is a list, only flows with names in that list AND matching source
    are marked for deletion. If delete_logic is anything other than "all" or a list,
    no flows are deleted.
    """
    matching_flows = df[df[merge_column] == source]
    
    if delete_logic == "all":
        # Delete all flows with matching source
        return matching_flows.index.tolist()
    
    elif isinstance(delete_logic, list):
        # Delete flows with names in the list that have matching source
        flows_to_delete = matching_flows[matching_flows['Flow'].isin(delete_logic)]
        return flows_to_delete.index.tolist()
    
    else:
        # Don't delete any flows
        return []


def _insert_flow_at_position(df: pd.DataFrame, 
                            new_flow: pd.Series, 
                            position: int) -> pd.DataFrame:
    """
    Helper function to insert a new flow at a specific position in the DataFrame.
    
    This function converts the DataFrame to a list of dictionaries, inserts the new
    flow at the specified position, and converts back to a DataFrame. This approach
    preserves the order of flows while maintaining all column data.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to insert the new flow into
    new_flow : pandas.Series
        New flow data to insert (should have the same columns as df)
    position : int
        Index position where the new flow should be inserted
        
    Returns
    -------
    pandas.DataFrame
        DataFrame with the new flow inserted at the specified position
        
    Notes
    -----
    The function temporarily converts the DataFrame to a list of dictionaries for
    insertion, then converts back. This ensures the new flow is placed exactly at
    the specified position while preserving all other flows and their order.
    """
    # Convert to list for easier manipulation
    df_list = df.to_dict('records')
    
    # Insert the new flow at the specified position
    df_list.insert(position, new_flow.to_dict())
    
    # Convert back to DataFrame
    return pd.DataFrame(df_list)


def validate_merge_parameters(df: pd.DataFrame, 
                             merge_source: str, 
                             value_1_merge: Union[str, List[str]], 
                            value_2_merge: Union[str, List[str]],
                             merge_column: str = 'Source') -> bool:
    """
    Validate parameters for the merge_flows function.
    
    This function performs validation checks to ensure that the merge_flows function
    can execute successfully. It checks for the existence of the merge source and
    validates that any specified flow names in merge logic lists actually exist.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to validate against
    merge_source : str
        Source value to merge (must exist in the merge_column)
    value_1_merge : str or list
        Value 1 merge logic (will be validated if it's a list)
    value_2_merge : str or list
        Value 2 merge logic (will be validated if it's a list)
    merge_column : str, optional
        Column name to use for matching flows (default: 'Source')
    
    Returns
    -------
    bool
        True if all parameters are valid, False otherwise
        
    Notes
    -----
    If merge logic parameters are lists, the function checks that all flow names
    in those lists exist in the DataFrame for the specified merge source.
    Warnings are printed for any validation failures.
    """
    # Check if source exists
    if merge_source not in df[merge_column].values:
        print(f"Warning: Source '{merge_source}' not found in DataFrame")
        return False
    
    # Check if flow names in lists exist
    for merge_logic, column_name in [(value_1_merge, 'Value 1'), (value_2_merge, 'Value 2')]:
        if isinstance(merge_logic, list):
            matching_flows = df[df[merge_column] == merge_source]
            missing_flows = [name for name in merge_logic if name not in matching_flows['Flow'].values]
            if missing_flows:
                print(f"Warning: Flows {missing_flows} not found for {column_name} merge")
                return False
    
    return True


def validate_finalize_parameters(df: pd.DataFrame, 
                                reference_flow: str, 
                                reference_source: str) -> bool:
    """
    Validate parameters for the finalize_df function.
    
    This function performs validation checks to ensure that the finalize_df function
    can execute successfully. It checks for the existence of required columns and
    validates that the specified reference flow exists in the DataFrame.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to validate (must contain required columns)
    reference_flow : str
        Name of the reference flow (must exist in the 'Flow' column)
    reference_source : str
        Source of the reference flow (must exist in the 'Source' column)
    
    Returns
    -------
    bool
        True if all parameters are valid, False otherwise
        
    Notes
    -----
    Required columns: ['Flow', 'Source', 'In/Out', 'Flow_Type', 'LCA Unit', 'LCA Amount']
    The function checks that the reference flow exists with the specified source
    before proceeding with the finalization process.
    """
    # Check if required columns exist
    required_columns = ['Flow', 'Source', 'In/Out', 'Flow_Type', 'LCA Unit', 'LCA Amount']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"Error: Missing required columns: {missing_columns}")
        return False
    
    # Check if reference flow exists
    reference_mask = (df['Flow'] == reference_flow) & (df['Source'] == reference_source)
    if not df[reference_mask].any().any():
        print(f"Error: Reference flow '{reference_flow}' from '{reference_source}' not found")
        return False
    
    return True


def get_finalize_summary(df: pd.DataFrame) -> dict:
    """
    Get a comprehensive summary of the finalized LCA DataFrame.
    
    This function provides various statistics about the finalized DataFrame including
    flow counts, input/output breakdowns, and flow type distributions. It's useful
    for understanding the structure and content of the processed LCA data.
    
    Parameters
    ----------
    df : pandas.DataFrame
        Finalized DataFrame with columns: ['Flow_Name', 'LCA_Amount', 'LCA_Unit', 
        'Is_Input', 'Reference_Product', 'Flow_Type', 'Category', 'Context', 'UUID', 'Description']
    
    Returns
    -------
    dict
        Dictionary containing summary statistics with keys:
        - total_flows: Total number of flows
        - input_flows: Number of input flows
        - output_flows: Number of output flows
        - reference_products: Number of reference product flows
        - unique_flow_types: Number of unique flow types
        - total_lca_amount: Sum of all LCA amounts
        - flow_type_breakdown: Dictionary of flow type counts
        
    Notes
    -----
    The summary provides both numerical counts and categorical breakdowns to help
    understand the distribution and characteristics of the LCA flows.
    """
    summary = {
        'total_flows': len(df),
        'input_flows': len(df[df['Is_Input'] == True]),
        'output_flows': len(df[df['Is_Input'] == False]),
        'reference_products': len(df[df['Reference_Product'] == True]),
        'unique_flow_types': df['Flow_Type'].nunique(),
        'total_lca_amount': df['LCA_Amount'].sum()
    }
    
    # Add flow type breakdown
    flow_type_counts = df['Flow_Type'].value_counts().to_dict()
    summary['flow_type_breakdown'] = flow_type_counts
    
    return summary

  
if __name__ == "__main__":
    # Run example usage
    finalized_df = main(reference_flow='99.85% REO Product', reference_source='Roaster Product', water_type='raw fresh water')
    print("Finalized DataFrame:")
    print(finalized_df)
    print("\n" + "="*60 + "\n")