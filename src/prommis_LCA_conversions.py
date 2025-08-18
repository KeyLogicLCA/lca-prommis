import pandas as pd
from pyomo.environ import (
    units,
    value,
)

# Recommended imports for molecular weight calculations
try:
    import pubchempy as pcp
    from pymatgen.core import Composition
    PYCHEMPY_AVAILABLE = True
except ImportError:
    print("Warning: pubchempy and/or pymatgen not available. Molecular weight conversions will be disabled.")
    PYCHEMPY_AVAILABLE = False
    

# Global dictionary for mapping units to display names
unit_display_mapping = {
    'kg': 'kg',
    'L': 'L',
    'l': 'L',  # Handle lowercase l
    'mol': 'mol',
    'kW*hr': 'kWh',
    'kW*h': 'kWh',  # Handle alternative format
    'MJ': 'MJ',
    'm3': 'm3',
    'kBq': 'kBq'
}

"""
PrOMMiS LCA Unit Conversion Module

This module converts PrOMMiS flowsheet data from various units to LCA-relevant units
for Life Cycle Assessment analysis. It handles complex unit conversions, mass/mole
fractions, and provides automatic molecular weight lookups for chemical compounds.

Key Features:
- Converts flow rates to total amounts over specified time periods
- Handles mass and mole fractions to calculate actual component amounts
- Automatically determines appropriate LCA units based on flow categories
- Provides molecular weight lookups via PubChem API for molar conversions
- Supports various input unit formats and special cases
- Maps output units to standardized LCA display formats

Main Functions:
- main(): Executes the complete conversion workflow and saves results
- convert_flows_to_lca_units(): Core conversion function with comprehensive unit handling
- get_molar_mass(): Retrieves molecular weights from PubChem database
- parse_unit_to_pyomo(): Converts unit strings to Pyomo unit expressions
- get_unit(): Resolves individual unit strings to Pyomo units

Unit Categories:
- Mass flows: Converted to kg (default LCA unit)
- Volume flows: Converted to L (or m3 for water)
- Energy flows: Electricity to kWh, heat to MJ
- Molar flows: Converted to kg using molecular weights
- Radioactivity: Converted to kBq

Usage:
    from prommis_LCA_conversions import main
    
    # Convert flows to LCA units with default settings
    df = main()
    
    # Customize conversion parameters
    df = convert_flows_to_lca_units(
        df, 
        hours=24,           # Convert to 24-hour period
        mol_to_kg=True,     # Convert moles to kg
        water_unit='m3'     # Use cubic meters for water
    )

Dependencies:
- pandas: Data manipulation and CSV handling
- pyomo.environ: Unit system and conversion capabilities
- pubchempy: Chemical compound lookups (optional)
- pymatgen: Molecular weight calculations (optional)

Notes:
- Requires internet connection for PubChem molecular weight lookups
- Handles special cases like embedded values in unit strings
- Provides comprehensive error handling for conversion failures
- Automatically maps output units to standardized display formats
"""


def main():
    """
    Execute the complete LCA unit conversion workflow.
    
    This function reads the raw LCA DataFrame, converts all flows to LCA-relevant units,
    and saves the results to a new CSV file. It serves as the main entry point
    for the unit conversion process.
    
    Returns
    -------
    pandas.DataFrame
        DataFrame with all flows converted to LCA units, including new columns:
        - 'LCA Amount': Converted amounts in appropriate LCA units
        - 'LCA Unit': Corresponding units (kg, L, kWh, MJ, mol, etc.)
        
    Notes
    -----
    - Automatically reads from 'lca_df.csv' in the current directory
    - Saves results to 'lca_df_converted.csv' in the current directory
    - Uses default conversion parameters (1 hour period, moles to kg, water in m3)
    - Handles all unit conversions automatically based on flow categories
    """
    df = pd.read_csv('lca_df.csv')
    df = convert_flows_to_lca_units(df)
    df.to_csv('lca_df_converted.csv', index=False)
    return df


# SECTION 1: Main function to convert prommis flows in a dataframe to LCA-relevant units
# Define a function that loops through a dataframe containing results from PrOMMiS
# The function then checks the category and unit of each flow and applies the appropriate conversion function
# The function will add two new columns to the dataframe: 'LCA Amount' and 'LCA Unit'
# Evidently, the final output will be the flows (in and out) in LCA-relevant units

def convert_flows_to_lca_units(df, hours=1, mol_to_kg=True, water_unit='m3'):
    """
    Convert PrOMMiS flowsheet flows to LCA-relevant units using Pyomo's unit system.
    
    This function processes a DataFrame containing PrOMMiS flowsheet data and converts
    all flows to consistent LCA units (kg, L, kWh, MJ, mol). It handles various unit
    formats, mass/mole fractions, concentrations, and special cases like embedded
    values in unit strings.
    
    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame with columns: ['Flow', 'Source', 'In/Out', 'Category', 
        'Value 1', 'Unit 1', 'Value 2', 'Unit 2']
    hours : float, optional
        Time period for conversion (default: 1). Multiplies all amounts by this value
        to convert from flow rates to total amounts.
    mol_to_kg : bool, optional
        Whether to convert molar amounts to mass using molecular weights (default: True).
        Requires internet connection for PubChem lookups.
    water_unit : string, optional
        What unit to use for water flows. Can be 'm3', 'L', or 'kg' (default: 'm3').
    
    Returns
    -------
    pandas.DataFrame
        Original DataFrame with two new columns:
        - 'LCA Amount': Converted amounts in LCA-relevant units
        - 'LCA Unit': Corresponding units (kg, L, kWh, MJ, mol)
    
    Notes
    -----
    - Converts mass fractions and mole fractions to actual amounts
    - Automatically determines target units based on flow category:
        * Water/wastewater: kg (if concentration given), L (if volume), or specified unit
        * Electricity: kWh
        * Heat: MJ
        * Molar flows: kg (if mol_to_kg=True) or mol
        * Radioactivity: kBq
        * All others: kg
    - Uses PubChem API for molecular weight lookups when mol_to_kg=True
    - Provides comprehensive error handling for conversion failures
    - If using mass/mole fractions, the first value should be the total stream amount, 
      and the second value should be the composition of the individual flow
    - Handles special cases like embedded values in unit strings (e.g., "5*mg/L")
    
    Examples
    --------
    >>> df = pd.read_csv('lca_df.csv')
    >>> # Convert to 24-hour period with moles converted to kg
    >>> converted_df = convert_flows_to_lca_units(df, hours=24, mol_to_kg=True)
    >>> print(converted_df[['Flow', 'LCA Amount', 'LCA Unit']])
    
    >>> # Customize water units and disable molar conversions
    >>> converted_df = convert_flows_to_lca_units(
    ...     df, hours=1, mol_to_kg=False, water_unit='L'
    ... )
    
    Raises
    ------
    Various conversion errors may occur and are handled gracefully:
    - Unit parsing errors: Logged and row skipped
    - Conversion failures: Logged with fallback to 0
    - Molecular weight lookup failures: Falls back to mol units
    """

    # Prepare output columns
    lca_amounts = []
    lca_units = []

    # Main processing loop: Convert each flow to LCA-relevant units
    # This loop handles unit parsing, value conversion, and category-based unit selection
    for idx, row in df.iterrows():
        unit1 = str(row['Unit 1']).strip()
        unit2 = str(row['Unit 2']).strip()
        
        # Parse and validate input values, handling special cases and data types
        # This section ensures robust handling of various input formats and edge cases
        try:
            value1 = float(row['Value 1']) if pd.notna(row['Value 1']) else 0.0
        except (ValueError, TypeError):
            value1 = 0.0
            
        try:
            if pd.isna(row['Value 2']) or row['Value 2'] == '' or str(row['Value 2']).strip() == '':
                value2 = None
            else:
                value2_str = str(row['Value 2'])
                # Handle special case where value contains "*mg/l" format
                if '*mg/l' in value2_str.lower() or '*mg/L' in value2_str.lower():
                    # Extract the numeric part before the asterisk
                    value2 = float(value2_str.split('*')[0])
                else:
                    value2 = float(row['Value 2'])
        except (ValueError, TypeError):
            value2 = None
        
        # Parse units and create Pyomo unit expressions for mathematical operations
        # This enables unit-aware calculations and conversions
        pyomo_unit1 = parse_unit_to_pyomo(unit1)
        if pyomo_unit1 is None:
            # If no valid unit, skip this row or use a default
            lca_amounts.append(0)
            lca_units.append('')
            continue
        
        # Start with Value 1 * Unit 1
        expression = value1 * pyomo_unit1
        
        # Handle mass and mole fractions to calculate actual component amounts
        # This section converts composition data to absolute quantities
        if value2 is not None and unit2:
            if unit2.lower() == 'mass fraction':
                # For mass fraction, multiply by the fraction to get actual mass
                expression = expression * value2
            elif unit2.lower() == 'mole fraction':
                # For mole fraction, multiply by the fraction to get actual moles
                expression = expression * value2
            else:
                # Add Value 2 * Unit 2 if applicable
                pyomo_unit2 = parse_unit_to_pyomo(unit2)
                if pyomo_unit2 is not None and isinstance(value2, (int, float)):
                    expression = expression * value2 * pyomo_unit2
                else:
                    print(f'Could not parse {unit2}')
        
        # Convert flow rates to total amounts over the specified time period
        # This is essential for LCA analysis which requires total inputs/outputs
        expression = expression * hours * units.hr
        
        # Determine target unit based on category and unit types
        # This ensures consistent LCA units across different flow categories
        category = str(row['Category']).lower()
        
        # Water and wastewater should be kg if a concentration is given
        # Otherwise, they should be L
        # All water will be converted to the desired unit later
        if 'water' in category.lower() or 'wastewater' in category.lower():
            if 'g' in unit1.lower() or 'g' in unit2.lower():
                target_unit = units.kg
            else:
                target_unit = units.L
        # Electricity should be kWh
        elif 'electricity' in category.lower():
            target_unit = units.kW * units.hr  # kWh
        # Heat should be MJ
        elif 'heat' in category.lower():
            target_unit = units.MJ
        # If the unit is mol, we'll convert to kg later
        elif 'mol' in unit1.lower():
            target_unit = units.mol
        # Any other unit should be kg
        elif 'Bq' in unit1.lower() or 'Ci' in unit1.lower():
            target_unit = units.kBq
        else:
            target_unit = units.kg
        
        # Perform the actual unit conversion using Pyomo's conversion system
        # This section handles the mathematical conversion and special cases
        try:
            converted_expression = units.convert(expression, to_units=target_unit)
            
            # Handle water flows with flexible unit specification
            # Water can be converted to m3, L, or kg based on user preference
            if 'water' in category and not 'wastewater' in category:
                val = value(converted_expression)
                
                if water_unit.lower() == 'm3':
                    new_unit = 'm3'
                    val /= 1000
                # No conversion required: 1 kg water = 1 L water
                # PrOMMiS gives water density as 1 kg/L, so using 1 kg/L here allows for consistency/compatability
                elif water_unit.lower() == 'l':
                    new_unit = 'L'
                elif water_unit.lower() == 'kg':
                    new_unit = 'kg'
                else:
                    new_unit = 'm3'
                    val /= 1000
            
            # Wastewater is only accepted as kg typically
            elif 'wastewater' in category.lower():
                val = value(converted_expression)
                new_unit = 'kg'
            
            # Convert molar flows to mass using molecular weights from PubChem
            # This enables consistent mass-based LCA analysis
            elif 'mol' in unit1.lower() and mol_to_kg:
                molar_mass = get_molar_mass(row['Flow'])
                if molar_mass is not None:
                    # Molar mass is in g/mol, so we need to convert to kg/mol by dividing by 1000
                    converted_expression = (converted_expression * molar_mass 
                                            / 1000 * units.kg / units.mol)
                    val = value(converted_expression)
                    new_unit = 'kg'
                else:
                    # Fall back to mol if molar mass lookup fails
                    val = value(converted_expression)
                    new_unit = 'mol'
            else:
                val = value(converted_expression)
                new_unit = str(target_unit)
            
            # Map output units to standardized display names for consistency
            # This ensures uniform unit representation across the LCA dataset
            new_unit = unit_display_mapping.get(new_unit, new_unit)
            
        except Exception as e:
            print(f"Error converting row {idx}: {e}")
            val = 0
            new_unit = ''
        
        lca_amounts.append(val)
        lca_units.append(new_unit)

    df['LCA Amount'] = lca_amounts
    df['LCA Unit'] = lca_units
    return df


# SECTION 2: Helper functions

# Helper function to get the molar mass of a compound
def get_molar_mass(compound_name):
    """
    Retrieve the molar mass of a compound using PubChem API.
    
    This function searches the PubChem database for chemical compounds and calculates
    their molecular weights using the molecular formula. It provides automatic
    molecular weight lookups for converting molar flows to mass flows in LCA analysis.
    
    Parameters
    ----------
    compound_name : str
        Name of the compound to look up (e.g., 'Carbon dioxide', 'Sulfuric acid')
        
    Returns
    -------
    float or None
        Molar mass in g/mol if found, None if compound not found or lookup fails
        
    Notes
    -----
    - Requires internet connection for PubChem API access
    - Returns None if pubchempy/pymatgen packages are not available
    - Uses the first compound found if multiple matches exist
    - Calculates molecular weight from molecular formula using pymatgen
    - Provides comprehensive error handling and warning messages
    
    Examples
    --------
    >>> molar_mass = get_molar_mass('Carbon dioxide')
    >>> print(f"CO2 molar mass: {molar_mass} g/mol")
    >>> # Output: CO2 molar mass: 44.01 g/mol
    
    >>> molar_mass = get_molar_mass('Sulfuric acid')
    >>> print(f"H2SO4 molar mass: {molar_mass} g/mol")
    >>> # Output: H2SO4 molar mass: 98.08 g/mol
    
    Raises
    ------
    Various exceptions may occur during API calls and are handled gracefully:
    - Connection errors: Logged and None returned
    - Compound not found: Warning printed and None returned
    - Formula parsing errors: Warning printed and None returned
    """
    if not PYCHEMPY_AVAILABLE:
        print(f"Warning: Cannot get molar mass for '{compound_name}' - pubchempy/pymatgen not available")
        return None
        
    try:
        compounds = pcp.get_compounds(compound_name, 'name')
        if not compounds:
            print(f"Warning: Compound '{compound_name}' not found in PubChem")
            return None
        
        compound = compounds[0]
        formula = compound.molecular_formula
        if not formula:
            print(f"Warning: No molecular formula found for '{compound_name}'")
            return None
            
        composition = Composition(formula)
        return composition.weight
    except Exception as e:
        print(f"Error getting molar mass for '{compound_name}': {e}")
        return None

# Helper function to parse units and create Pyomo unit expression
def parse_unit_to_pyomo(unit_str):
    """
    Convert unit string to Pyomo unit expression.
    
    This function parses unit strings and converts them to Pyomo unit expressions
    that can be used for unit conversions and calculations. It handles complex
    unit combinations including multiplication, division, and exponents.
    
    Parameters
    ----------
    unit_str : str
        Unit string to parse (e.g., 'kg/hr', 'mg/L', 'hp', 'kW*hr')
        
    Returns
    -------
    pyomo.environ.units.Unit or None
        Pyomo unit expression if successfully parsed, None for mass/mole fractions
        or empty strings
        
    Notes
    -----
    - Returns None for mass/mole fractions and empty strings
    - Recognizes these fraction keywords: 'mass fraction', 'mole fraction', 'mol fraction',
      'mass frac', 'mole frac', 'mol frac'
    - Handles multiplication using '*' symbol
    - Handles division using '/' symbol  
    - Handles exponents using '**' or '^' symbols
    - Does not recognize parentheses in unit strings
    - May not handle units not recognized by Pyomo
    
    Examples
    --------
    >>> parse_unit_to_pyomo('kg/hr')
    <pyomo unit: kg/hr>
    
    >>> parse_unit_to_pyomo('mg/L')
    <pyomo unit: mg/L>
    
    >>> parse_unit_to_pyomo('kW*hr')
    <pyomo unit: kW*hr>
    
    >>> parse_unit_to_pyomo('mass fraction')
    None
    
    >>> parse_unit_to_pyomo('')
    None
    
    Limitations
    -----------
    - No support for parentheses in unit strings
    - Limited to units recognized by Pyomo
    - Complex unit expressions may fail to parse
    - Error handling provides fallback to default values
    """
    if not unit_str or unit_str == '' or unit_str.lower() in ['mass fraction', 'mole fraction', 'mol fraction', 'mass frac', 'mole frac', 'mol frac']:
        return None
    
    unit_str = unit_str.strip()
    # Replace all '**' with '^' so that we can split the unit string by '*' for multiplication
    unit_str = unit_str.replace('**', '^')
    
    # Split the unit string into a list of strings being multiplied, then into a list of strings being divided
    unit_strings = unit_str.split('*')
    first_unit = True # The first substring will be used to initialize the total_unit
    for string in unit_strings:
        string = string.strip()
        string = string.split('/')
        
        i = 0
        for sub_string in string:
            sub_string = sub_string.strip()
            # If the substring is empty, we skip it
            if len(sub_string) == 0:
                continue
            if first_unit:
                # If the first unit is a division, we need to invert the unit
                if unit_str[0] == '/':
                    total_unit = get_unit(sub_string) ** (-1)
                else:
                    total_unit = get_unit(sub_string)
                first_unit = False
            # The first string is the numerator, the rest are the denominators
            elif i == 0:
                total_unit *= get_unit(sub_string)
            else:
                total_unit /= get_unit(sub_string)
            
            i += 1
    
    return total_unit

# Helper function to obtain the Pyomo unit for a string
def get_unit(string, default=None):
    """
    Resolve individual unit strings to Pyomo unit objects.
    
    This function attempts to convert unit strings to Pyomo unit objects by trying
    various parsing strategies. It handles different unit formats including those
    with exponents and provides fallback options for unrecognized units.
    
    Parameters
    ----------
    string : str
        Unit string to resolve (e.g., 'kg', 'hr', 'kW', 'm3')
    default : any, optional
        Default value to return if unit resolution fails (default: None)
        
    Returns
    -------
    pyomo.environ.units.Unit or any
        Pyomo unit object if successfully resolved, default value if resolution fails
        
    Notes
    -----
    - First attempts direct attribute lookup from Pyomo units
    - Handles exponent notation by converting '^' to '**'
    - Automatically adds '**' for numeric exponents (e.g., 'm3' becomes 'm**3')
    - Falls back to lowercase unit names if initial lookup fails
    - Provides comprehensive error messages for debugging
    - Returns default value for any resolution failures
    
    Examples
    --------
    >>> get_unit('kg')
    <pyomo unit: kg>
    
    >>> get_unit('m3')
    <pyomo unit: m**3>
    
    >>> get_unit('kW')
    <pyomo unit: kW>
    
    >>> get_unit('unknown_unit', default='kg')
    'kg'
    
    Error Handling
    --------------
    - Unrecognized units: Error message printed, default returned
    - Malformed unit strings: Error message printed, default returned
    - Missing Pyomo units: Error message printed, default returned
    
    The function prints detailed error messages to help identify unit parsing issues.
    """
    unit_str = string.strip()
    try:
        unit_str = unit_str.replace('^', '**')
        new_unit = getattr(units, unit_str, None)
        if new_unit == None:
            for index, char in enumerate(unit_str):
                if char.isdigit():
                    unit_str = unit_str[:index] + '**' + unit_str[index:]
                    new_unit = getattr(units, unit_str, None)
                    continue
            if new_unit == None:
                unit_str = unit_str.lower()
                new_unit = getattr(units, unit_str, None)
                if new_unit == None:
                    raise AttributeError
        return new_unit
    except AttributeError:
        print(f'Error parsing unit string {string}.')
        print(f'Ignoring this unit and returning {default}.')
        print('Make sure you check that the unit string is correct, including capitalization and exponents, and that there are no parentheses.')
        return default


"""
Main Workflow and Important Considerations

This module performs the following key operations:

1. Unit Parsing and Conversion:
   - Parses complex unit strings (e.g., 'kg/hr', 'mg/L', 'kW*hr')
   - Converts to Pyomo unit expressions for mathematical operations
   - Handles special cases like mass/mole fractions and embedded values

2. Flow Rate to Total Amount Conversion:
   - Multiplies all flows by the specified time period (hours)
   - Converts from per-hour rates to total amounts over the period
   - Essential for LCA analysis which requires total material/energy inputs

3. Category-Based Unit Standardization:
   - Water flows: Converted to specified units (m3, L, or kg)
   - Energy flows: Electricity to kWh, heat to MJ
   - Mass flows: Standardized to kg (default LCA unit)
   - Molar flows: Converted to kg using molecular weights

4. Special Case Handling:
   - Mass/mole fractions: Multiplied to get actual component amounts
   - Embedded values: Extracts numeric values from strings like "5*mg/L"
   - Error handling: Graceful fallbacks for conversion failures

Key Features:
- Automatic molecular weight lookups via PubChem API
- Comprehensive unit mapping and standardization
- Robust error handling with informative messages
- Flexible water unit specification
- Support for various input unit formats

Usage Notes:
- Ensure internet connection for molecular weight lookups
- Check unit string formats for compatibility
- Monitor error messages for conversion issues
- Verify output units match LCA software requirements

For troubleshooting unit conversion issues, check:
- Unit string format and spelling
- Pyomo unit availability
- Internet connectivity for PubChem lookups
- Input data quality and completeness
"""


if __name__ == "__main__":
    df = main()
    print("Converted LCA DataFrame:")
    print(df)
    print("\n" + "="*60 + "\n")
