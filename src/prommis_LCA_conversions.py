# This script includes the conversion functions for the PrOMMiS data units to LCA-relevant units

# Function to convert W to kWh
def convert_w_to_kwh(power_watts, hours):
    """
    Convert power in watts to energy in kilowatt-hours.
    
    :param power_watts: Power in watts (W)
    :param hours: Time in hours
    :return: Energy in kilowatt-hours (kWh)
    """
    return (power_watts / 1000) * hours

# Function to convert kw to kWh
def convert_kw_to_kwh(power_kw, hours):
    """
    Convert power in kilowatts to energy in kilowatt-hours.
    
    :param power_kw: Power in kilowatts (kW)
    :param hours: Time in hours
    :return: Energy in kilowatt-hours (kWh)
    """
    return power_kw * hours


# Function to convert m3/hour to liters
def convert_m3_per_hour_to_liters(m3_per_hour, hours):
    """
    Convert flow rate from cubic meters per hour to liters per hour.
    
    :param m3_per_hour: Flow rate in cubic meters per hour (m3/h)
    :return: Flow rate in liters per hour (L/h)
    """
    return m3_per_hour * hours * 1000  # 1 m3 = 1000 liters

# Function to convert m3/hour to m3
def convert_m3_per_hour_to_m3(m3_per_hour, hours):
    """
    Convert flow rate from cubic meters per hour to cubic meters.
    
    :param m3_per_hour: Flow rate in cubic meters per hour (m3/h)
    :return: Flow rate in cubic meters (m3)
    """
    return m3_per_hour * hours  


#Function to estimate solid mass from flow rate 
def estimate_mass_from_flow_rate(flow_rate_m3_per_hour, density_kg_per_m3, hours):
    """
    Estimate solid mass from flow rate and density.
    
    Can be applied for both input solids, solid waste, and output solid products

    :param flow_rate_m3_per_hour: Flow rate in cubic meters per hour (m3/h)
    :param density_kg_per_m3: Density in kilograms per cubic meter (kg/m3)
    :param hours: Time in hours
    :return: Estimated solid mass in kilograms (kg)
    """
    return flow_rate_m3_per_hour * density_kg_per_m3 * hours  # kg

# Function to convert kg/h to kg
def estimate_mass_from_mass_flow_rate(flow_rate_kg_per_hour, hours):
    """
    Estimate solid mass from mass flow rate.
    
    :param flow_rate_kg_per_hour: Flow rate in kilograms per hour (kg/h)
    :param hours: Time in hours
    :return: Estimated solid mass in kilograms (kg)
    """
    return flow_rate_kg_per_hour * hours  # kg

#-------------------------------------------------------------------------------------------
# Function to read a dataframe and convert all flows to LCA-relevant units
# First define default units for each flow category
def get_flow_conversion_specs():
    """
    Define conversion specifications for each flow element.
    
    :return: Dictionary with flow elements as keys and a dict specifying
             category, target unit, original unit, and conversion function name as values.
    """
    return {
        'Electricity': {
            'category': 'Electricity',
            'target_unit': 'kWh',
            'original_unit': 'W',
            'function': 'convert_w_to_kwh'
        },
        'Electricity': {
            'category': 'Electricity',
            'target_unit': 'kWh',
            'original_unit': 'kw',
            'function': 'convert_kw_to_kwh'
        },
        'Water': {
            'category': 'Water',
            'target_unit': 'L',
            'original_unit': 'm3/hour',
            'function': 'convert_m3_per_hour_to_liters'
        },
        'Wastewater': {
            'category': 'Wastewater',
            'target_unit': 'L',
            'original_unit': 'm3/hour',
            'function': 'convert_m3_per_hour_to_liters'
        },
        'Solid Inputs': {
            'category': 'Solid Inputs',
            'target_unit': 'kg',
            'original_unit': 'm3/hour, kg/m3',
            'function': 'estimate_mass_from_flow_rate'
        },
        'Chemicals': {
            'category': 'Chemicals',
            'target_unit': 'kg',
            'original_unit': 'm3/hour, kg/m3',
            'function': 'estimate_mass_from_flow_rate'  
        },
        'Solid Waste': {
            'category': 'Solid Waste',
            'target_unit': 'kg',
            'original_unit': 'kg/hour',
            'function': 'estimate_mass_from_mass_flow_rate'
        },
        'Solid Products': {
            'category': 'Solid Products',
            'target_unit': 'kg',
            'original_unit': 'kg/hour',
            'function': 'estimate_mass_from_mass_flow_rate'
        }
    }

def convert_flows_to_lca_units(df):
    