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


def main():
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
        What unit to use for water flows. Can be m3, L, or kg (default: m3).
    
    Returns
    -------
    pandas.DataFrame
        Original DataFrame with two new columns:
        - 'LCA Amount': Converted amounts in LCA-relevant units
        - 'LCA Unit': Corresponding units (kg, L, kWh, MJ, mol)
    
    Notes
    -----
    - Converts mass fractions and mole fractions to actual amounts
    - Automatically determines target units based on flow category
    - Uses PubChem API for molecular weight lookups when mol_to_kg=True
    - Provides error handling for conversion failures
    - If using mass/mole fractions, the first value should be the total stream amount, 
      and the second value should be the composition of the individual flow.
    
    Examples
    --------
    >>> df = pd.read_csv('lca_df.csv')
    >>> converted_df = convert_flows_to_lca_units(df, hours=24)
    >>> print(converted_df[['Flow', 'LCA Amount', 'LCA Unit']])
    """

    # Prepare output columns
    lca_amounts = []
    lca_units = []

    # Loop through each row
    for idx, row in df.iterrows():
        unit1 = str(row['Unit 1']).strip()
        unit2 = str(row['Unit 2']).strip()
        
        # Convert values to appropriate types, handling empty strings and special cases
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
        
        # Create the base expression with Value 1 and Unit 1
        pyomo_unit1 = parse_unit_to_pyomo(unit1)
        #TODO: make this a better error handling system. May need to do something in finalize_LCA_flows or do some error message or logging file
        if pyomo_unit1 is None:
            # If no valid unit, skip this row or use a default
            lca_amounts.append(0)
            lca_units.append('')
            continue
        
        # Start with Value 1 * Unit 1
        expression = value1 * pyomo_unit1
        
        # Handle mass fraction and mole fraction
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
        
        # Multiply by hours to get total amount
        expression = expression * hours * units.hr
        
        # Determine target unit based on category and unit types
        category = str(row['Category']).lower()
        
        # Water and wastewater should be kg if a concentration is given
        # Otherwise, they should be L
        # All water will be converted to L or kg later
        if 'water' in category or 'wastewater' in category:
            if 'g' in unit1.lower() or 'g' in unit2.lower():
                target_unit = units.kg
            else:
                target_unit = units.L
        # Electricity should be kWh
        elif 'electricity' in category:
            target_unit = units.kW * units.hr  # kWh
        # Heat should be MJ
        elif 'heat' in category:
            target_unit = units.MJ
        # If the unit is mol, we'll convert to kg later
        elif 'mol' in unit1.lower():
            target_unit = units.mol
        # Any other unit should be kg
        elif 'Bq' in unit1.lower() or 'Ci' in unit1.lower():
            target_unit = units.kBq
        else:
            target_unit = units.kg
        
        # Convert to target unit
        try:
            converted_expression = units.convert(expression, to_units=target_unit)
            
            if 'water' in category or 'wastewater' in category:
                val = value(converted_expression)
                
                if water_unit == 'm3':
                    new_unit = 'm3'
                    val /= 1000
                # No conversion required: 1 kg water = 1 L water
                # PrOMMiS gives water density as 1 kg/L, so using 1 kg/L here allows for consistency/compatability
                elif water_unit == 'L':
                    new_unit = 'L'
                elif water_unit == 'kg':
                    new_unit = 'kg'
                else:
                    new_unit = 'm3'
                    val /= 1000
            
            # If the target unit is mol and mol_to_kg is True, convert to kg
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
            
            # Clean up unit string for display
            #TODO: make this a global dictionary
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
    Returns the molar mass of a compound using PubChem API.
    
    Parameters
    ----------
    compound_name : str
        Name of the compound to look up
        
    Returns
    -------
    float
        Molar mass in g/mol
        
    Notes
    -----
    Requires internet connection for PubChem API access.
    Returns None if compound not found or lookup fails.
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
    
    Parameters
    ----------
    unit_str : str
        Unit string to parse (e.g., 'kg/hr', 'mg/L', 'hp')
        
    Returns
    -------
    pyomo.environ.units.Unit or None
        Pyomo unit expression if successfully parsed, None otherwise
        
    Notes
    -----
    Returns None for mass/mole fractions and empty strings.
    This function does not recognize parentheses in the unit string. 
    All multiplication should use the '*' symbol.
    All division should use the '/' symbol.
    All exponents should use the '**' or '^' symbol.
    These are the only symbols allowed in the unit string.
    This function may not handle units not recognized by Pyomo.
    For a mass/mole fraction, the unit should be one of the following: mass fraction, mole fraction, mol fraction, mass frac, mole frac, mol frac
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


if __name__ == "__main__":
    df = main()
    print("Converted LCA DataFrame:")
    print(df)
    print("\n" + "="*60 + "\n")
