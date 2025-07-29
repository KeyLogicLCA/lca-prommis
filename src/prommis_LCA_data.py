# This script includes the functions to get the LCA data from the PrOMMiS flowsheet

import pandas as pd
from pyomo.environ import (
    units,
    value,
)
from warnings import warn
import prommis.uky.uky_flowsheet as uky

def main():
    m, results = uky.main()
    df = get_lca_df(m)  
    return df

def get_lca_df(m):
    """
    Create a pandas DataFrame with LCA-relevant flows organized for analysis.
    
    Args:
        m: pyomo model
        
    Returns:
        pandas.DataFrame: DataFrame with LCA flows and their properties
    """
    # Initialize empty lists to store data
    flow_id = []
    flow = []
    source = []
    in_out = []
    category = []
    value_1 = []
    unit_1 = []
    value_2 = []
    unit_2 = []
    
    current_id = 1
    
    # Helper function to safely get values
    def safe_value(var, default=0):
        try:
            if value(var) == 0:
                print(f"Error: {var} is 0")
            return value(var)
        except Exception:
            print(f"Error: {var} is not a valid variable")
            return default
    
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
    metal_mass_frac = {
        "Al2O3": 26.98 * 2 / (26.98 * 2 + 16 * 3),
        "Fe2O3": 55.845 * 2 / (55.845 * 2 + 16 * 3),
        "CaO": 40.078 / (40.078 + 16),
        "Sc2O3": 44.956 * 2 / (44.956 * 2 + 16 * 3),
        "Y2O3": 88.906 * 2 / (88.906 * 2 + 16 * 3),
        "La2O3": 138.91 * 2 / (138.91 * 2 + 16 * 3),
        "Ce2O3": 140.12 * 2 / (140.12 * 2 + 16 * 3),
        "Pr2O3": 140.91 * 2 / (140.91 * 2 + 16 * 3),
        "Nd2O3": 144.24 * 2 / (144.24 * 2 + 16 * 3),
        "Sm2O3": 150.36 * 2 / (150.36 * 2 + 16 * 3),
        "Gd2O3": 157.25 * 2 / (157.25 * 2 + 16 * 3),
        "Dy2O3": 162.5 * 2 / (162.5 * 2 + 16 * 3),
    }
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
    # 1. Solid Feed components
    solid_feed_mass = value(units.convert(m.fs.leach_solid_feed.flow_mass[0], to_units=units.kg / units.hr))
    
    # To print the mass of the REEs coming in to calculate recovery
    ree_mass_in = 0
    
    for flow_name, comp in solid_components:
        try:
            mass_frac = safe_value(m.fs.leach_solid_feed.mass_frac_comp[0, comp])
            mass_flow = mass_frac * solid_feed_mass
            
            flow_id.append(current_id)
            flow.append(flow_name)
            source.append("Solid Feed")
            in_out.append("In")
            category.append("Material")
            value_1.append(solid_feed_mass)
            unit_1.append("kg/hr")
            value_2.append(mass_frac)
            unit_2.append("mass fraction")
            current_id += 1
            
            if flow_name in ree_oxides:
                ree_mass_in += mass_flow * metal_mass_frac[comp]
        except Exception:
            print(f"Error: could not process solid feed {flow_name}")
    
    print(f"REE mass in: {ree_mass_in} kg/hr")
    
    # 2. Liquid Feed components
    liquid_feed_vol = safe_value(m.fs.leach_liquid_feed.flow_vol[0])
    
    # Water
    try:
        h2o_conc = safe_value(m.fs.leach_liquid_feed.conc_mass_comp[0, "H2O"])
        flow_id.append(current_id)
        flow.append("Water")
        source.append("Liquid Feed")
        in_out.append("In")
        category.append("Material")
        value_1.append(liquid_feed_vol)
        unit_1.append("L/hr")
        value_2.append(h2o_conc)
        unit_2.append("mg/L")
        current_id += 1
    except Exception:
        print(f"Error: could not process liquid feed water")
    
    # Sulfuric Acid (H + SO4)
    try:
        h_conc = safe_value(m.fs.leach_liquid_feed.conc_mass_comp[0, "H"])
        so4_conc = safe_value(m.fs.leach_liquid_feed.conc_mass_comp[0, "SO4"])
        total_acid_conc = h_conc + so4_conc
        
        flow_id.append(current_id)
        flow.append("Sulfuric Acid")
        source.append("Liquid Feed")
        in_out.append("In")
        category.append("Material")
        value_1.append(liquid_feed_vol)
        unit_1.append("L/hr")
        value_2.append(total_acid_conc)
        unit_2.append("mg/L")
        current_id += 1
    except Exception:
        print(f"Error: could not process liquid feed sulfuric acid")
    
    # 3. Rougher Organic Make-up
    rougher_org_vol = safe_value(m.fs.rougher_org_make_up.flow_vol[0])
    
    # Kerosene
    try:
        kerosene_conc = safe_value(m.fs.rougher_org_make_up.conc_mass_comp[0, "Kerosene"])
        flow_id.append(current_id)
        flow.append("Kerosene")
        source.append("Rougher Organic Make-up")
        in_out.append("In")
        category.append("Material")
        value_1.append(rougher_org_vol)
        unit_1.append("L/hr")
        value_2.append(kerosene_conc)
        unit_2.append("mg/L")
        current_id += 1
    except Exception:
        print(f"Error: could not process rougher organic make-up kerosene")
    
    # DEHPA
    try:
        dehpa_conc = safe_value(m.fs.rougher_org_make_up.conc_mass_comp[0, "DEHPA"])
        
        flow_id.append(current_id)
        flow.append("DEHPA")
        source.append("Rougher Organic Make-up")
        in_out.append("In")
        category.append("Material")
        value_1.append(rougher_org_vol)
        unit_1.append("L/hr")
        value_2.append(dehpa_conc)
        unit_2.append("mg/L")
        current_id += 1
    except Exception:
        print(f"Error: could not process rougher organic make-up DEHPA")
    
    # 4. Cleaner Organic Make-up
    cleaner_org_vol = safe_value(m.fs.cleaner_org_make_up.flow_vol[0])
    
    # Kerosene
    try:
        kerosene_conc = safe_value(m.fs.cleaner_org_make_up.conc_mass_comp[0, "Kerosene"])
        flow_id.append(current_id)
        flow.append("Kerosene")
        source.append("Cleaner Organic Make-up")
        in_out.append("In")
        category.append("Material")
        value_1.append(cleaner_org_vol)
        unit_1.append("L/hr")
        value_2.append(kerosene_conc)
        unit_2.append("mg/L")
        current_id += 1
    except Exception:
        print(f"Error: could not process cleaner organic make-up kerosene")
    
    # DEHPA
    try:
        dehpa_conc = safe_value(m.fs.cleaner_org_make_up.conc_mass_comp[0, "DEHPA"])
        
        flow_id.append(current_id)
        flow.append("DEHPA")
        source.append("Cleaner Organic Make-up")
        in_out.append("In")
        category.append("Material")
        value_1.append(cleaner_org_vol)
        unit_1.append("L/hr")
        value_2.append(dehpa_conc)
        unit_2.append("mg/L")
        current_id += 1
    except Exception:
        print(f"Error: could not process cleaner organic make-up DEHPA")
    
    # 5. Acid Feed 1
    acid1_vol = safe_value(m.fs.acid_feed1.flow_vol[0])
    
    # Water
    try:
        h2o_conc = safe_value(m.fs.acid_feed1.conc_mass_comp[0, "H2O"])
        flow_id.append(current_id)
        flow.append("Water")
        source.append("Acid Feed 1")
        in_out.append("In")
        category.append("Material")
        value_1.append(acid1_vol)
        unit_1.append("L/hr")
        value_2.append(h2o_conc)
        unit_2.append("mg/L")
        current_id += 1
    except Exception:
        print(f"Error: could not process acid feed 1 water")
    
    # Hydrochloric Acid (H + Cl)
    try:
        h_conc = safe_value(m.fs.acid_feed1.conc_mass_comp[0, "H"])
        cl_conc = safe_value(m.fs.acid_feed1.conc_mass_comp[0, "Cl"])
        total_acid_conc = h_conc + cl_conc
        
        flow_id.append(current_id)
        flow.append("Hydrochloric Acid")
        source.append("Acid Feed 1")
        in_out.append("In")
        category.append("Material")
        value_1.append(acid1_vol)
        unit_1.append("L/hr")
        value_2.append(total_acid_conc)
        unit_2.append("mg/L")
        current_id += 1
    except Exception:
        print(f"Error: could not process acid feed 1 hydrochloric acid")
    
    # 6. Acid Feed 2
    acid2_vol = safe_value(m.fs.acid_feed2.flow_vol[0])
    
    # Water
    try:
        h2o_conc = safe_value(m.fs.acid_feed2.conc_mass_comp[0, "H2O"])
        flow_id.append(current_id)
        flow.append("Water")
        source.append("Acid Feed 2")
        in_out.append("In")
        category.append("Material")
        value_1.append(acid2_vol)
        unit_1.append("L/hr")
        value_2.append(h2o_conc)
        unit_2.append("mg/L")
        current_id += 1
    except Exception:
        print(f"Error: could not process acid feed 2 water")
    
    # Hydrochloric Acid (H + Cl)
    try:
        h_conc = safe_value(m.fs.acid_feed2.conc_mass_comp[0, "H"])
        cl_conc = safe_value(m.fs.acid_feed2.conc_mass_comp[0, "Cl"])
        total_acid_conc = h_conc + cl_conc
        
        flow_id.append(current_id)
        flow.append("Hydrochloric Acid")
        source.append("Acid Feed 2")
        in_out.append("In")
        category.append("Material")
        value_1.append(acid2_vol)
        unit_1.append("L/hr")
        value_2.append(total_acid_conc)
        unit_2.append("mg/L")
        current_id += 1
    except Exception:
        print(f"Error: could not process acid feed 2 hydrochloric acid")
    
    # 7. Acid Feed 3
    acid3_vol = safe_value(m.fs.acid_feed3.flow_vol[0])
    
    # Water
    try:
        h2o_conc = safe_value(m.fs.acid_feed3.conc_mass_comp[0, "H2O"])
        flow_id.append(current_id)
        flow.append("Water")
        source.append("Acid Feed 3")
        in_out.append("In")
        category.append("Material")
        value_1.append(acid3_vol)
        unit_1.append("L/hr")
        value_2.append(h2o_conc)
        unit_2.append("mg/L")
        current_id += 1
    except Exception:
        print(f"Error: could not process acid feed 3 water")
    
    # Hydrochloric Acid (H + Cl)
    try:
        h_conc = safe_value(m.fs.acid_feed3.conc_mass_comp[0, "H"])
        cl_conc = safe_value(m.fs.acid_feed3.conc_mass_comp[0, "Cl"])
        total_acid_conc = h_conc + cl_conc
        
        flow_id.append(current_id)
        flow.append("Hydrochloric Acid")
        source.append("Acid Feed 3")
        in_out.append("In")
        category.append("Material")
        value_1.append(acid3_vol)
        unit_1.append("L/hr")
        value_2.append(total_acid_conc)
        unit_2.append("mg/L")
        current_id += 1
    except Exception:
        print(f"Error: could not process acid feed 3 hydrochloric acid")
    
    # 8. Electricity streams
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
                flow_id.append(current_id)
                flow.append("Electricity")
                source.append(source_name)
                in_out.append("In")
                category.append("Energy")
                value_1.append(power_val)
                unit_1.append("hp")
                value_2.append("")
                unit_2.append("")
                current_id += 1
            except Exception:
                print(f"Error: could not process {source_name} electricity")
    
    # 9. Heat streams
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
                
                flow_id.append(current_id)
                flow.append("Heat")
                source.append(source_name)
                in_out.append("In")
                category.append("Energy")
                value_1.append(heat_val)
                unit_1.append("W")
                value_2.append("")
                unit_2.append("")
                current_id += 1
            except Exception:
                print(f"Error: could not process {source_name} heat")
    
    # 10. REE Product components
    try:
        product_mass = value(units.convert(m.fs.roaster.flow_mass_product[0], to_units=units.kg / units.hr))
        
        # To print the mass of the REEs coming out to calculate recovery
        ree_mass_out = 0
        
        for flow_name, comp in product_components:
            try:
                # Multiply by molar mass of the oxide, convert to kg/hr
                mass_flow = value(units.convert(m.fs.roaster.flow_mol_comp_product[0, comp] * molar_mass[f'{comp}2O3'], 
                                                to_units=units.kg / units.hr))
                mass_frac = mass_flow / product_mass
                
                flow_id.append(current_id)
                flow.append(flow_name)
                source.append("Roaster Product")
                in_out.append("Out")
                category.append("Product")
                value_1.append(product_mass)
                unit_1.append("kg/hr")
                value_2.append(mass_frac)
                unit_2.append("mass fraction")
                current_id += 1
                
                if flow_name in ree_elements:
                    ree_mass_out += mass_flow * metal_mass_frac[f'{comp}2O3']
            except Exception:
                print(f"Error: could not process roaster product {flow_name}")
    except Exception:
        print(f"Error: could not process roaster product")
    
    print(f'REE mass out: {ree_mass_out} kg/hr')
    
    # 11. Gas Emissions
    gas_components = [
        ("Oxygen", "O2"),
        ("Water Vapor", "H2O"),
        ("Carbon Dioxide", "CO2"),
        ("Nitrogen", "N2")
    ]
    
    for flow_name, comp in gas_components:
        try:
            total_flow = safe_value(m.fs.roaster.gas_outlet.flow_mol[0])
            mol_frac = safe_value(m.fs.roaster.gas_outlet.mole_frac_comp[0, comp])
            
            flow_id.append(current_id)
            flow.append(flow_name)
            source.append("Roaster Emissions")
            in_out.append("Out")
            category.append("Waste")
            value_1.append(total_flow)
            unit_1.append("mol/hr")
            value_2.append(mol_frac)
            unit_2.append("mole fraction")
            current_id += 1
        except Exception:
            print(f"Error: could not process roaster emissions {flow_name}")
    
    # 12. Solid Waste
    solid_waste_streams = [
        ("Filter Cake", getattr(m.fs, 'leach_filter_cake', None)),
        ("Precipitate", getattr(m.fs, 'precipitate', None)),
        ("Dust and Volatiles", getattr(m.fs, 'dust_and_volatiles', None))
    ]
    
    for waste_name, waste_var in solid_waste_streams:
        if waste_var is not None:
            try:
                if waste_name == "Filter Cake":
                    waste_val = safe_value(waste_var.flow_mass[0])
                    waste_unit = "kg/hr"
                elif waste_name == "Precipitate":
                    waste_val = safe_value(waste_var[0])
                    waste_unit = "ton/hr"
                else:  # Dust and Volatiles
                    waste_val = safe_value(waste_var[0])
                    waste_unit = "ton/hr"
                
                flow_id.append(current_id)
                flow.append(waste_name)
                source.append("Process")
                in_out.append("Out")
                category.append("Waste")
                value_1.append(waste_val)
                unit_1.append(waste_unit)
                value_2.append("")
                unit_2.append("")
                current_id += 1
            except Exception:
                print(f"Error: could not process {waste_name}")
    
    # 13. Liquid Waste
    liquid_waste_streams = [
        ("Precipitate Purge", getattr(m.fs, 'precip_purge', None)),
        ("Load Separator Purge", getattr(m.fs.load_sep, 'purge', None)),
        ("Scrub Separator Purge", getattr(m.fs.scrub_sep, 'purge', None)),
        ("Leach Filter Cake Liquid", getattr(m.fs, 'leach_filter_cake_liquid', None)),
        ("Roaster Liquid Inlet", getattr(m.fs.roaster, 'liquid_inlet', None))
    ]
    
    for waste_name, waste_var in liquid_waste_streams:
        if waste_var is not None:
            try:
                if waste_name == "Roaster Liquid Inlet":
                    waste_val = safe_value(waste_var.flow_vol[0])
                else:
                    waste_val = safe_value(waste_var.flow_vol[0])
                
                flow_id.append(current_id)
                flow.append(waste_name)
                source.append("Process")
                in_out.append("Out")
                category.append("Waste")
                value_1.append(waste_val)
                unit_1.append("L/hr")
                value_2.append("")
                unit_2.append("")
                current_id += 1
            except Exception:
                print(f"Error: could not process {waste_name}")
    
    # 14. Organic Solvent Waste
    organic_waste_streams = [
        ("Rougher Circuit Purge", getattr(m.fs, 'sc_circuit_purge', None)),
        ("Cleaner Circuit Purge", getattr(m.fs, 'cleaner_purge', None))
    ]
    
    for waste_name, waste_var in organic_waste_streams:
        if waste_var is not None:
            try:
                waste_val = safe_value(waste_var.flow_vol[0])
                
                flow_id.append(current_id)
                flow.append(waste_name)
                source.append("Process")
                in_out.append("Out")
                category.append("Waste")
                value_1.append(waste_val)
                unit_1.append("L/hr")
                value_2.append("")
                unit_2.append("")
                current_id += 1
            except Exception:
                print(f"Error: could not process {waste_name}")
    
    # Create DataFrame
    df = pd.DataFrame({
        'Flow_ID': flow_id,
        'Flow': flow,
        'Source': source,
        'In/Out': in_out,
        'Category': category,
        'Value 1': value_1,
        'Unit 1': unit_1,
        'Value 2': value_2,
        'Unit 2': unit_2
    })
    
    print(f'Total product mass out: {safe_value(m.fs.roaster.flow_mass_product[0])} kg/hr')
    print(f'Product purity: {ree_mass_out / value(units.convert(m.fs.roaster.flow_mass_product[0], to_units=units.kg / units.hr)) * 100}%')
    print(f'Recovery: {ree_mass_out / ree_mass_in * 100}%')
    
    return df


if __name__ == "__main__":
    df = main()
    print(df)
    df.to_csv("lca_df.csv")
    warn(
        "Recent changes to this UKy flowsheet have made the underlying process more realistic, but the REE recovery values have fallen as a result."
    )
    warn(
        "Efforts are ongoing to increase the REE recovery while keeping the system as realistic as possible. https://github.com/prommis/prommis/issues/152 in the PrOMMiS repository is tracking the status of this issue."
    )
