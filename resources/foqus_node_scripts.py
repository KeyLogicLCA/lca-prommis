# olca_node_script
# Last Reviewed: 02/10/2026 - 4:30 PM

import pandas as pd
import olca_schema as olca

from netlolca.NetlOlca import NetlOlca

import src as lca_prommis

params = my_parameters.copy() # get a copy of the parameters df

for count, row in params.iterrows():
    if x[row['parameter_description']] is not None:
        row['parameter_value'] = float(x[row['parameter_description']]) 
    else:
        row['parameter_value'] = 0

parameter_set_name = "Baseline" # get the parameter_set name

param_set_ref = lca_prommis.run_analysis.update_parameter ( netl, 
                                                            ps_uuid, 
                                                            parameter_set_name, 
                                                            params)

result = lca_prommis.run_analysis.run_analysis (netl, 
                                                ps_uuid, 
                                                impact_method_uuid, 
                                                param_set_ref)
result.wait_until_ready()
total_impacts = lca_prommis.generate_total_results.generate_total_results(result)

# save the total impacts to the node outputs
for result in total_impacts:
    f[result['name']] = result['value']


# TODO: add 'parameter_description' column to params df                     --> Done
# TODO: standardize flows naming in openLCA and parameters_df               --> Done
# TODO: make sure the code returns the parameter set name before
#       we get to the olca_node script update the parameters in openLCA     --> Done
# TODO: make sure the same impact category names are used in both:          --> Done
#       'total_impacts' and the f[] node output variables


# prommis node script
# Last Reviewed: 02/10/2026 - x:xx PM

import os
import pandas as pd
import olca_schema as olca
from netlolca.NetlOlca import NetlOlca
import prommis.uky.uky_flowsheet as uky 
import src as lca_prommis

from pyomo.environ import TransformationFactory
from idaes.core.util.model_diagnostics import DiagnosticsToolbox
from prommis.uky.costing.ree_plant_capcost import QGESSCostingData
import prommis.uky.uky_flowsheet as uky

home_dir = os.path.expanduser("~")

m = uky.build()

uky.set_operating_conditions(m)

if "Leach liquid feed" in x:
	m.fs.leach_liquid_feed.flow_vol.fix(x["Leach liquid feed"])

if "split_fraction" in x:
	m.fs.load_sep.split_fraction[0.0, 'recycle'].fix(x["split_fraction"])

uky.set_scaling(m)

scaling = TransformationFactory("core.scale_model")
scaled_model = scaling.create_using(m, rename=False)

if uky.degrees_of_freedom(scaled_model) != 0:
    raise AssertionError("Degrees of freedom != 0")

uky.initialize_system(scaled_model)
uky.solve_system(scaled_model)

uky.fix_organic_recycle(scaled_model)
scaled_results = uky.solve_system(scaled_model)

if not uky.check_optimal_termination(scaled_results):
    raise RuntimeError("Solver failed to terminate optimally")

# Propagate results back to original model
results = scaling.propagate_solution(scaled_model, m)

# 5. Add Costing (Optional, but likely needed for optimization)
uky.add_costing(m)

# Costing initialization
QGESSCostingData.costing_initialization(m.fs.costing)
QGESSCostingData.initialize_fixed_OM_costs(m.fs.costing)
QGESSCostingData.initialize_variable_OM_costs(m.fs.costing)

# Final solve with costing
uky.solve_system(m)

prommis_data = lca_prommis.data_lca.get_lca_df(m)

df = lca_prommis.convert_lca.convert_flows_to_lca_units(prommis_data, hours=1, mol_to_kg=True, water_unit='m3')

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

df = lca_prommis.final_lca.merge_flows(df, merge_source='Solid Feed', new_flow_name='374 ppm REO Feed', value_2_merge=REO_list)


df = lca_prommis.final_lca.merge_flows(df, merge_source='Roaster Product', new_flow_name='73.4% REO Product')


df = lca_prommis.final_lca.merge_flows(df, merge_source='Wastewater', new_flow_name='Wastewater', merge_column='Category') 

df = lca_prommis.final_lca.merge_flows(df, merge_source='Solid Waste', new_flow_name='Solid Waste', merge_column='Category') 

finalized_df = lca_prommis.final_lca.finalize_df(
        df=df, 
        reference_flow='73.4% REO Product', 
        reference_source='Roaster Product',
        water_type='raw fresh water'
    )

output_dir = os.path.join(home_dir, 'output')

if not os.path.exists(output_dir):
	os.makedirs(output_dir)

finalized_df.to_csv(os.path.join(output_dir, "finalized_df.csv"), index=False)

for _, row in finalized_df.iterrows():
    f[row['Flow_Name']] = row['LCA_Amount']