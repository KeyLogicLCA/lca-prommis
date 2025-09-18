# This script includes the functions to get the LCA data from the PrOMMiS flowsheet
# Source for Sodium Hydroxide: https://netl.doe.gov/projects/files/DF_Stage1_O_Rare_Earth_Leaching_2014-01.pdf
# Source for Oxalic Acid: https://netl.doe.gov/projects/files/DF_Stage1_O_Rare_earth_oxide_formation_2014-01.pdf

import pandas as pd
from pyomo.environ import (
    units,
    value,
)
from warnings import warn
import prommis.uky.uky_flowsheet as uky

"""
PrOMMiS LCA Data Extraction Module

This module extracts Life Cycle Assessment (LCA) relevant data from the PrOMMiS UKy flowsheet
model. It processes the Pyomo model results to create a comprehensive DataFrame containing
all material flows, energy inputs, and waste outputs needed for LCA analysis.

Key Features:
- Extracts solid feed components including REE oxides and impurities
- Processes liquid feed streams (water, acids, organic chemicals)
- Captures energy inputs (electricity, heat) from various unit operations
- Identifies product streams (REE oxides) and waste streams
- Calculates mass balances and recovery rates

Main Functions:
- main(): Executes the complete workflow and saves results to CSV
- get_lca_df(): Extracts and organizes LCA data from the Pyomo model

Data Sources:
- Sodium Hydroxide: NETL UP library acid leaching unit process
- Oxalic Acid: NETL UP library REE oxide formation unit process

Usage:
    from prommis_LCA_data import main
    
    # Extract LCA data and save to CSV
    df = main()
    
    # Access the DataFrame directly
    print(df.head())

Dependencies:
- pandas: Data manipulation and CSV export
- pyomo.environ: Access to model variables and units
- prommis.uky.uky_flowsheet: UKy flowsheet model
- warnings: Warning system for model issues
"""


def main():
    """
    Execute the complete LCA data extraction workflow from the PrOMMiS UKy flowsheet.
    
    This function runs the UKy flowsheet model, extracts all LCA-relevant data,
    and saves the results to a CSV file. It serves as the main entry point
    for the LCA data extraction process.
    
    Returns
    -------
    pandas.DataFrame
        DataFrame containing all extracted LCA flows with columns:
        ['Flow', 'Source', 'In/Out', 'Category', 'Value 1', 'Unit 1', 'Value 2', 'Unit 2']
    """
    m, _ = uky.main()
    df = get_lca_df(m)
    df.to_csv("output/lca_df.csv")
    return df


def get_lca_df(m):
    """
    Extract and organize LCA-relevant data from the PrOMMiS Pyomo model.
    
    This function processes the solved Pyomo model to extract all material flows,
    energy inputs, and waste outputs needed for Life Cycle Assessment. It handles
    multiple data categories including solid feeds, liquid feeds, chemicals,
    energy streams, products, and waste streams.
    
    Parameters
    ----------
    m : pyomo.environ.ConcreteModel
        Solved Pyomo model containing the UKy flowsheet results
        
    Returns
    -------
    pandas.DataFrame
        DataFrame with LCA flows organized by the following categories:
        
        Solid Inputs:
        - REE oxides (Y2O3, La2O3, Ce2O3, Pr2O3, Nd2O3, Sm2O3, Gd2O3, Dy2O3)
        - Impurities (Al2O3, Fe2O3, CaO, Sc2O3, Inerts)
        
        Liquid Inputs:
        - Water from various feed streams
        - Sulfuric acid (H2SO4) from leach liquid feed
        - Hydrochloric acid (HCl) from acid feeds 1-3
        - Organic chemicals (Kerosene, DEHPA) from rougher/cleaner make-up
        
        Energy Inputs:
        - Electricity from mixers (leach, rougher, cleaner, precipitator)
        - Heat from roaster and solution heater
        
        Chemicals (Proxy Data):
        - Sodium hydroxide (calculated from sulfuric acid ratio)
        - Oxalic acid (calculated from sulfuric acid ratio)
        
        Products:
        - REE oxides from roaster product
        
        Waste Streams:
        - Gas emissions (O2, H2O, CO2, N2) from roaster
        - Solid waste (filter cake, dust and volatiles)
        - Liquid waste (various purge streams and filtrates)
        
        Columns: ['Flow', 'Source', 'In/Out', 'Category', 'Value 1', 'Unit 1', 'Value 2', 'Unit 2']
        
    Notes
    -----
    - Calculates mass balances and recovery rates for REEs
    - Uses proxy data for sodium hydroxide and oxalic acid based on NETL UP library
    - Processes both direct model outputs and calculated derived values
    - Includes comprehensive error handling for missing or invalid model variables
    """
    # Initialize empty lists to store data
    flow = []
    source = []
    in_out = []
    category = []
    value_1 = []
    unit_1 = []
    value_2 = []
    unit_2 = []
    
    # Helper function to safely get values
    def safe_value(var, default=0):
        """
        Safely extract values from Pyomo variables with error handling.
        
        This helper function attempts to extract values from Pyomo model variables
        while providing graceful error handling for invalid or missing variables.
        It also checks for zero values which may indicate model issues.
        
        Parameters
        ----------
        var : pyomo.core.base.var.SimpleVar or similar
            Pyomo variable to extract value from
        default : float, optional
            Default value to return if extraction fails (default: 0)
            
        Returns
        -------
        float
            Extracted value from the variable, or default if extraction fails
            
        Notes
        -----
        - Prints error messages for debugging when variables are invalid
        - Warns about zero values which may indicate model convergence issues
        - Returns the default value for any exceptions during value extraction
        """
        try:
            if value(var) == 0:
                print(f"Error: {var} is 0")
            return value(var)
        except Exception:
            print(f"Error: {var} is not a valid variable")
            return default
    
    # Define component lists for processing
    # Solid components include REE oxides and impurities found in the feed
    solid_components = [
        ("Inerts", "inerts"),
        ("Scandium Oxide", "Sc2O3"),
        ("Yttrium Oxide", "Y2O3"),
        ("Lanthanum Oxide", "La2O3"),
        ("Cerium Oxide", "Ce2O3"),
        ("Praseodymium Oxide", "Pr2O3"),
        ("Neodymium Oxide", "Nd2O3"),
        ("Samarium Oxide", "Sm2O3"),
        ("Gadolinium Oxide", "Gd2O3"),
        ("Dysprosium Oxide", "Dy2O3"),
        ("Aluminum Oxide", "Al2O3"),
        ("Calcium Oxide", "CaO"),
        ("Iron Oxide", "Fe2O3")
    ]
    ree_oxides = [
        "Yttrium Oxide",
        "Lanthanum Oxide",
        "Cerium Oxide",
        "Praseodymium Oxide",
        "Neodymium Oxide",
        "Samarium Oxide",
        "Gadolinium Oxide",
        "Dysprosium Oxide",
    ]
    # Inerts, scandium, aluminum, calcium, and iron are not listed in the product components.
    # There seems to be mass that is not accounted for in the product components.
    product_components = [
        ("Yttrium", "Y"),
        ("Lanthanum", "La"),
        ("Cerium", "Ce"),
        ("Praseodymium", "Pr"),
        ("Neodymium", "Nd"),
        ("Samarium", "Sm"),
        ("Gadolinium", "Gd"),
        ("Dysprosium", "Dy")
    ]
    ree_elements = [
        "Yttrium",
        "Lanthanum",
        "Cerium",
        "Praseodymium",
        "Neodymium",
        "Samarium",
        "Gadolinium",
        "Dysprosium"
    ]
    
    # Molar masses for converting between elemental and oxide forms
    # Used for calculating mass flows and fractions in product streams
    molar_mass = {
        "Al2O3": (26.98 * 2 + 16 * 3) * units.g / units.mol,
        "Fe2O3": (55.845 * 2 + 16 * 3) * units.g / units.mol,
        "CaO": (40.078 + 16) * units.g / units.mol,
        "Sc2O3": (44.956 * 2 + 16 * 3) * units.g / units.mol,
        "Y2O3": (88.906 * 2 + 16 * 3) * units.g / units.mol,
        "La2O3": (138.91 * 2 + 16 * 3) * units.g / units.mol,
        "Ce2O3": (140.12 * 2 + 16 * 3) * units.g / units.mol,
        "Pr2O3": (140.91 * 2 + 16 * 3) * units.g / units.mol,
        "Nd2O3": (144.24 * 2 + 16 * 3) * units.g / units.mol,
        "Sm2O3": (150.36 * 2 + 16 * 3) * units.g / units.mol,
        "Gd2O3": (157.25 * 2 + 16 * 3) * units.g / units.mol,
        "Dy2O3": (162.5 * 2 + 16 * 3) * units.g / units.mol,
    }
    
    # 1. Process Solid Feed Components
    # Extract mass fractions and flows for all solid components entering the system
    # This includes REE oxides and impurities that will be processed through leaching
    solid_feed_mass = value(units.convert(m.fs.leach_solid_feed.flow_mass[0], to_units=units.kg / units.hr))
    
    # To print the mass of the REEs coming in to calculate recovery
    reo_mass_in = 0
    
    for flow_name, comp in solid_components:
        try:
            mass_frac = safe_value(m.fs.leach_solid_feed.mass_frac_comp[0, comp])
            mass_flow = mass_frac * solid_feed_mass
            
            flow.append(flow_name)
            source.append("Solid Feed")
            in_out.append("In")
            category.append("Solid Input")
            value_1.append(solid_feed_mass)
            unit_1.append("kg/hr")
            value_2.append(mass_frac)
            unit_2.append("mass fraction")
            
            if flow_name in ree_oxides:
                reo_mass_in += mass_flow
        except Exception:
            print(f"Error: could not process solid feed {flow_name}")
    
    print(f"REE mass in: {reo_mass_in} kg/hr")
    
    # 2. Process Liquid Feed Components
    # Extract water and acid concentrations from the leach liquid feed stream
    # This stream provides the aqueous medium for the leaching process
    liquid_feed_vol = safe_value(m.fs.leach_liquid_feed.flow_vol[0])
    
    # Water
    try:
        h2o_conc = safe_value(m.fs.leach_liquid_feed.conc_mass_comp[0, "H2O"])
        flow.append("Water")
        source.append("Liquid Feed")
        in_out.append("In")
        category.append("Water")
        value_1.append(liquid_feed_vol)
        unit_1.append("L/hr")
        value_2.append(h2o_conc)
        unit_2.append("mg/L")
    except Exception:
        print(f"Error: could not process liquid feed water")
    
    # Sulfuric Acid (H + SO4)
    try:
        h_conc = safe_value(m.fs.leach_liquid_feed.conc_mass_comp[0, "H"])
        so4_conc = safe_value(m.fs.leach_liquid_feed.conc_mass_comp[0, "SO4"])
        total_sulfuric_conc = h_conc + so4_conc
        
        flow.append("Sulfuric Acid")
        source.append("Liquid Feed")
        in_out.append("In")
        category.append("Chemicals")
        value_1.append(liquid_feed_vol)
        unit_1.append("L/hr")
        value_2.append(total_sulfuric_conc)
        unit_2.append("mg/L")
    except Exception:
        print(f"Error: could not process liquid feed sulfuric acid")
    
    # 3. Process Rougher Organic Make-up
    # Extract organic solvent and extractant flows for the rougher flotation circuit
    # These chemicals are essential for REE separation and concentration
    rougher_org_vol = safe_value(m.fs.rougher_org_make_up.flow_vol[0])
    
    # Kerosene
    try:
        kerosene_conc = safe_value(m.fs.rougher_org_make_up.conc_mass_comp[0, "Kerosene"])
        flow.append("Kerosene")
        source.append("Rougher Organic Make-up")
        in_out.append("In")
        category.append("Chemicals")
        value_1.append(rougher_org_vol)
        unit_1.append("L/hr")
        value_2.append(kerosene_conc)
        unit_2.append("mg/L")
    except Exception:   
        print(f"Error: could not process rougher organic make-up kerosene")
    
    # DEHPA    
    try:
        dehpa_conc = safe_value(m.fs.rougher_org_make_up.conc_mass_comp[0, "DEHPA"])
        flow.append("DEHPA")
        source.append("Rougher Organic Make-up")
        in_out.append("In")
        category.append("Chemicals")
        value_1.append(rougher_org_vol)
        unit_1.append("L/hr")
        value_2.append(dehpa_conc)
        unit_2.append("mg/L")
    except Exception:
        print(f"Error: could not process rougher organic make-up DEHPA")
    
    # 4. Process Cleaner Organic Make-up
    # Extract organic solvent and extractant flows for the cleaner flotation circuit
    # This provides additional purification of the REE concentrate
    cleaner_org_vol = safe_value(m.fs.cleaner_org_make_up.flow_vol[0])
    
    # Kerosene
    try:
        kerosene_conc = safe_value(m.fs.cleaner_org_make_up.conc_mass_comp[0, "Kerosene"])
        flow.append("Kerosene")
        source.append("Cleaner Organic Make-up")
        in_out.append("In")
        category.append("Chemicals")
        value_1.append(cleaner_org_vol)
        unit_1.append("L/hr")
        value_2.append(kerosene_conc)
        unit_2.append("mg/L")
    except Exception:
        print(f"Error: could not process cleaner organic make-up kerosene")
    
    # DEHPA
    try:
        dehpa_conc = safe_value(m.fs.cleaner_org_make_up.conc_mass_comp[0, "DEHPA"])
        flow.append("DEHPA")
        source.append("Cleaner Organic Make-up")
        in_out.append("In")
        category.append("Chemicals")
        value_1.append(cleaner_org_vol)
        unit_1.append("L/hr")
        value_2.append(dehpa_conc)
        unit_2.append("mg/L")
    except Exception:
        print(f"Error: could not process cleaner organic make-up DEHPA")
    
    # 5. Process Acid Feed 1
    # Extract hydrochloric acid feed for the first acid addition stage
    # This acid is used for pH control and metal dissolution
    acid1_vol = safe_value(m.fs.acid_feed1.flow_vol[0])
    
    # Water
    try:
        h2o_conc = safe_value(m.fs.acid_feed1.conc_mass_comp[0, "H2O"])
        flow.append("Water")
        source.append("Acid Feed 1")
        in_out.append("In")
        category.append("Water")
        value_1.append(acid1_vol)
        unit_1.append("L/hr")
        value_2.append(h2o_conc)
        unit_2.append("mg/L")
    except Exception:
        print(f"Error: could not process acid feed 1 water")
    
    # Hydrochloric Acid (H + Cl)
    try:
        h_conc = safe_value(m.fs.acid_feed1.conc_mass_comp[0, "H"])
        cl_conc = safe_value(m.fs.acid_feed1.conc_mass_comp[0, "Cl"])
        total_acid_conc = h_conc + cl_conc
        
        flow.append("Hydrochloric Acid")
        source.append("Acid Feed 1")
        in_out.append("In")
        category.append("Chemicals")
        value_1.append(acid1_vol)
        unit_1.append("L/hr")
        value_2.append(total_acid_conc)
        unit_2.append("mg/L")
    except Exception:
        print(f"Error: could not process acid feed 1 hydrochloric acid")
    
    # 6. Process Acid Feed 2
    # Extract hydrochloric acid feed for the second acid addition stage
    # Additional acid may be needed for complete metal dissolution
    acid2_vol = safe_value(m.fs.acid_feed2.flow_vol[0])
    
    # Water
    try:
        h2o_conc = safe_value(m.fs.acid_feed2.conc_mass_comp[0, "H2O"])
        flow.append("Water")
        source.append("Acid Feed 2")
        in_out.append("In")
        category.append("Water")
        value_1.append(acid2_vol)
        unit_1.append("L/hr")
        value_2.append(h2o_conc)
        unit_2.append("mg/L")
    except Exception:
        print(f"Error: could not process acid feed 2 water")
    
    # Hydrochloric Acid (H + Cl)
    try:
        h_conc = safe_value(m.fs.acid_feed2.conc_mass_comp[0, "H"])
        cl_conc = safe_value(m.fs.acid_feed2.conc_mass_comp[0, "Cl"])
        total_acid_conc = h_conc + cl_conc
        
        flow.append("Hydrochloric Acid")
        source.append("Acid Feed 2")
        in_out.append("In")
        category.append("Chemicals")
        value_1.append(acid2_vol)
        unit_1.append("L/hr")
        value_2.append(total_acid_conc)
        unit_2.append("mg/L")
    except Exception:
        print(f"Error: could not process acid feed 2 hydrochloric acid")
    
    # 7. Process Acid Feed 3
    # Extract hydrochloric acid feed for the third acid addition stage
    # Final acid addition for pH adjustment and process optimization
    acid3_vol = safe_value(m.fs.acid_feed3.flow_vol[0])
    
    # Water
    try:
        h2o_conc = safe_value(m.fs.acid_feed3.conc_mass_comp[0, "H2O"])
        flow.append("Water")
        source.append("Acid Feed 3")
        in_out.append("In")
        category.append("Water")
        value_1.append(acid3_vol)
        unit_1.append("L/hr")
        value_2.append(h2o_conc)
        unit_2.append("mg/L")
    except Exception:
        print(f"Error: could not process acid feed 3 water")
    
    # Hydrochloric Acid (H + Cl)
    try:
        h_conc = safe_value(m.fs.acid_feed3.conc_mass_comp[0, "H"])
        cl_conc = safe_value(m.fs.acid_feed3.conc_mass_comp[0, "Cl"])
        total_acid_conc = h_conc + cl_conc
        
        flow.append("Hydrochloric Acid")
        source.append("Acid Feed 3")
        in_out.append("In")
        category.append("Chemicals")
        value_1.append(acid3_vol)
        unit_1.append("L/hr")
        value_2.append(total_acid_conc)
        unit_2.append("mg/L")
    except Exception:
        print(f"Error: could not process acid feed 3 hydrochloric acid")
        
    # # 8. Process Sodium Hydroxide (Proxy Data)
    # # Estimate sodium hydroxide consumption based on NETL UP library ratios
    # # This chemical is used for pH adjustment during the leaching process
    # # Source: NETL UP library acid leaching unit process
    # try:
    total_sulfuric = total_sulfuric_conc * safe_value(m.fs.leach_liquid_feed.flow_vol[0]) * units.mg/units.hr
    #     naoh_in = value(units.convert(total_sulfuric * 0.478/1.17, 
    #                             to_units=units.kg/units.hr))
    #     flow.append("Sodium Hydroxide")
    #     source.append("Acid Leaching")
    #     in_out.append("In")
    #     category.append("Chemicals")
    #     value_1.append(naoh_in)
    #     unit_1.append("kg/hr")
    #     value_2.append("")
    #     unit_2.append("")
    # except Exception:
    #     print(f"Error: could not process acid leaching sodium hydroxide")
    
    # 9. Process Oxalic Acid (Proxy Data)
    # Estimate oxalic acid consumption based on NETL UP library ratios
    # This chemical is used for REE precipitation and oxide formation
    # Source: NETL UP library REE oxide formation unit process
    # TODO: Move this calculation to EDX and clean up the spreadsheet
    # https://mykeylogic.sharepoint.com/:x:/r/sites/KL_PRJ_LCA-2300.203.014REEPreliminaryAssessment/_layouts/15/Doc.aspx?sourcedoc=%7BAD82A6A7-95D8-4408-B4E9-3ADB9E7024C8%7D&file=NETL%20UP%20Library%20Case%20Study%20Inventory.xlsx&action=default&mobileredirect=true
    # See cells O3:P5
    try:
        oxalic_in = value(units.convert(total_sulfuric * 0.762/1.728723404, 
                                to_units=units.kg/units.hr))
        flow.append("Oxalic Acid")
        source.append("Precipitation")
        in_out.append("In")
        category.append("Chemicals")
        value_1.append(oxalic_in)
        unit_1.append("kg/hr")
        value_2.append("")
        unit_2.append("")
    except Exception:
        print(f"Error: could not process precipitation oxalic acid")
    
    # 10. Process Electricity Consumption
    # Extract electrical power requirements from various mixing operations
    # These represent the mechanical energy inputs for process agitation
    electricity_sources = [
        ("Leach Mixer", getattr(m.fs.leach_mixer, 'power', None)),
        ("Rougher Mixer", getattr(m.fs.rougher_mixer, 'power', None)),
        ("Cleaner Mixer", getattr(m.fs.cleaner_mixer, 'power', None)),
        ("Precipitator Mixer", getattr(getattr(m.fs, 'precipitator_mixer', None), 'power', None))
    ]
    
    for source_name, power_var in electricity_sources:
        if power_var is not None:
            try:
                power_val = safe_value(power_var)
                flow.append("Electricity, AC, 120 V")
                source.append(source_name)
                in_out.append("In")
                category.append("Electricity")
                value_1.append(power_val)
                unit_1.append("hp")
                value_2.append("")
                unit_2.append("")
            except Exception:
                print(f"Error: could not process {source_name} electricity")
    
    # 11. Process Heat Consumption
    # Extract heat duty requirements from thermal operations
    # These represent the thermal energy inputs for roasting and solution heating
    heat_sources = [
        ("Roaster", getattr(m.fs.roaster, 'heat_duty', None)),
        ("Solution Heater", getattr(m.fs, 'leach_solution_heater', None))
    ]
    
    for source_name, heat_var in heat_sources:
        if heat_var is not None:
            try:
                if source_name == "Roaster":
                    heat_val = safe_value(heat_var[0])
                else:
                    heat_val = safe_value(heat_var.duty)
                
                flow.append("Heat")
                source.append(source_name)
                in_out.append("In")
                category.append("Heat")
                value_1.append(heat_val)
                unit_1.append("W")
                value_2.append("")
                unit_2.append("")
            except Exception:
                print(f"Error: could not process {source_name} heat")
    
    # 12. Process REE Product Components
    # Extract mass flows and fractions for the final REE oxide products
    # These represent the valuable outputs from the separation and purification process
    try:
        product_mass = value(units.convert(m.fs.roaster.flow_mass_product[0], to_units=units.kg / units.hr))
        
        # To print the mass of the REEs coming out to calculate recovery
        reo_mass_out = 0
        
        for flow_name, comp in product_components:
            try:
                # Multiply by molar mass of the oxide, convert to kg/hr
                mass_flow = value(units.convert(m.fs.roaster.flow_mol_comp_product[0, comp] * molar_mass[f'{comp}2O3'], 
                                                to_units=units.kg / units.hr))
                mass_frac = mass_flow / product_mass
                
                flow.append(f'{flow_name} Oxide')
                source.append("Roaster Product")
                in_out.append("Out")
                category.append("Solid Output")
                value_1.append(product_mass)
                unit_1.append("kg/hr")
                value_2.append(mass_frac)
                unit_2.append("mass fraction")
                
                if flow_name in ree_elements:
                    reo_mass_out += mass_flow
            except Exception:
                print(f"Error: could not process roaster product {flow_name}")
    except Exception:
        print(f"Error: could not process roaster product")
    
    print(f'REE mass out: {reo_mass_out} kg/hr')
    
    # 13. Process Gas Emissions from Roaster
    # Extract gas composition and flow rates from the roaster emissions
    # These emissions include process gases and combustion products
    gas_components = [
        ("Oxygen", "O2"),
        ("Water", "H2O"),
        ("Carbon dioxide", "CO2"),
        ("Nitrogen", "N2")
    ]
    
    for flow_name, comp in gas_components:
        try:
            total_flow = safe_value(m.fs.roaster.gas_outlet.flow_mol[0])
            mol_frac = safe_value(m.fs.roaster.gas_outlet.mole_frac_comp[0, comp])
            
            flow.append(flow_name)
            source.append("Roaster Emissions")
            in_out.append("Out")
            category.append("Emissions to air")
            value_1.append(total_flow)
            unit_1.append("mol/hr")
            value_2.append(mol_frac)
            unit_2.append("mole fraction")
        except Exception:
            print(f"Error: could not process roaster emissions {flow_name}")
    
    # 14. Process Solid Waste Streams
    # Extract solid waste flows including filter cake and dust/volatiles
    # These represent the non-recoverable solid materials from the process
    solid_waste_streams = [
        ("Filter Cake", getattr(m.fs, 'leach_filter_cake', None)),
        ("Dust and Volatiles", getattr(m.fs, 'dust_and_volatiles', None))
    ]
    
    for waste_name, waste_var in solid_waste_streams:
        if waste_var is not None:
            try:
                if waste_name == "Filter Cake":
                    waste_val = safe_value(waste_var.flow_mass[0])
                    waste_unit = "kg/hr"
                else:  # Dust and Volatiles
                    waste_val = safe_value(waste_var[0])
                    waste_unit = "ton/hr"
                
                flow.append(waste_name)
                source.append("Process")
                in_out.append("Out")
                category.append("Solid Waste")
                value_1.append(waste_val)
                unit_1.append(waste_unit)
                value_2.append("")
                unit_2.append("")
            except Exception:
                print(f"Error: could not process {waste_name}")
    
    # 15. Process Liquid Waste Streams
    # Extract various liquid waste flows including purge streams and filtrates
    # These represent aqueous waste streams that require treatment or disposal
    liquid_waste_streams = [
        ("Precipitate Purge", getattr(m.fs, 'precip_purge', None)),
        ("Load Separator Purge", getattr(m.fs.load_sep, 'purge', None)),
        ("Scrub Separator Purge", getattr(m.fs.scrub_sep, 'purge', None)),
        ("Leach Filter Cake Liquid", getattr(m.fs, 'leach_filter_cake_liquid', None)),
        ("Rougher Circuit Purge", getattr(m.fs, 'sc_circuit_purge', None)),
        ("Cleaner Circuit Purge", getattr(m.fs, 'cleaner_purge', None))
    ]
    
    for waste_name, waste_var in liquid_waste_streams:
        if waste_var is not None:
            try:
                waste_val = safe_value(waste_var.flow_vol[0])
                
                flow.append(waste_name)
                source.append("Process")
                in_out.append("Out")
                category.append("Wastewater")
                value_1.append(waste_val)
                unit_1.append("L/hr")
                value_2.append("")
                unit_2.append("")
            except Exception:
                print(f"Error: could not process {waste_name}")
    
    # Create the final DataFrame with all extracted LCA data
    # This DataFrame contains all material flows, energy inputs, and waste outputs
    df = pd.DataFrame({
        'Flow': flow,
        'Source': source,
        'In/Out': in_out,
        'Category': category,
        'Value 1': value_1,
        'Unit 1': unit_1,
        'Value 2': value_2,
        'Unit 2': unit_2
    })
    
    print(f'Product purity: {reo_mass_out / value(units.convert(m.fs.roaster.flow_mass_product[0], to_units=units.kg / units.hr)) * 100}%')
    print(f'Recovery: {reo_mass_out / reo_mass_in * 100}%')
    
    return df


if __name__ == "__main__":
    df = main()
    print(df)
    warn(
        "Recent changes to this UKy flowsheet have made the underlying process more realistic, but the REE recovery values have fallen as a result."
    )
    warn(
        "Efforts are ongoing to increase the REE recovery while keeping the system as realistic as possible. https://github.com/prommis/prommis/issues/152 in the PrOMMiS repository is tracking the status of this issue."
    )
