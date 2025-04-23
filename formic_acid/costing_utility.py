import numpy as np
import os
import json
from pyomo.common.fileutils import this_file_dir
from pyomo.environ import (
    ConcreteModel,
    TransformationFactory,
    SolverFactory,
    Var,
    Param,
    Constraint,
    Expression,
    Objective,
    value,
    log,
    exp,
    units as pyunits,
    assert_optimal_termination
)
from pyomo.network import Arc
from idaes.core import FlowsheetBlock, UnitModelBlock, UnitModelCostingBlock
from idaes.core.solvers import get_solver
from idaes.core.util import scaling as iscale
from idaes.core.util.initialization import propagate_state
from idaes.core.util.model_statistics import degrees_of_freedom
from idaes.core.util.model_diagnostics import DiagnosticsToolbox
from idaes.models.properties.modular_properties.base.generic_property import GenericParameterBlock
from gas_property import get_prop, EosType
from membrane_1d  import Membrane1D, MembraneFlowPattern
from idaes.core.initialization import (
    BlockTriangularizationInitializer,
)

from idaes.models.unit_models import (
    Compressor,
    IsentropicPressureChangerInitializer,
    Mixer,
    MomentumMixingType,
    Separator,
    SplittingType,
    SeparatorInitializer,
    HeatExchangerFlowPattern
)

from electrolyzer_submodels import ElectrochemicalCellModuleSimple
#from idaes.models_extra.power_generation.costing.power_plant_capcost import (
#    QGESSCosting,
#    QGESSCostingData,
#)

from power_plant_capcost import (
    QGESSCosting,
    QGESSCostingData,
)

__author__ = "Maojian Wang, Jinliang Ma, Brandon Paul"

pyunits.load_definitions_from_strings(
    [
        # custom units related to polymer layers plants
        # 1 lbmol = 1 g-mole * 453.59237 lb/g
        "lbmol = 453.59237 * mol",
        # https://toweringskills.com/financial-analysis/cost-indices/
        # 2018 value is 603.1, Dec 2021 value is 776.3
        # https://www.chemengonline.com/2021-cepci-updates-december-prelim-and-november-final/
        # Dec 2021 is 28.0% higher than Dec 2020
        # https://www.chemengonline.com/2020-cepci-updates-december-prelim-and-november-final/
        # Dec 2020 is 2.5% higher than Dec 2019
        # https://www.chemengonline.com/2019-cepci-updates-december-prelim-and-november-final/
        # Dec 2019 is 3.9% lower than Dec 2018
        # Dec 2021 = 776.3 = Dec 2018 * (1 - 3.9/100) * (1 + 2.5/100) * (1 + 28.0/100)
        # >>> Dec 2018 = 615.7
        "USD_2018_Dec = 615.7/500 * USD_CE500",
    ]
)



def build_costing(
        blk,
        # model decision flags
        distillation_type,
        cost_year,
        capacity_factor,
        ):

    blk.costing = QGESSCosting()
    blk.costing.capacity_factor = capacity_factor

    # check that the model solved properly and has 0 degrees of freedom
    assert degrees_of_freedom(blk) == 0

    ###########################################################################
    #  Create costing constraints                                             #
    ###########################################################################

    # polymer layer accounts have tech number 8 so that no native accounts are
    # unintentionally overwritten (e.g., some water system and balance-of-plant
    # accounts reuse the same account IDs)

    directory = this_file_dir()
    with open(os.path.join(directory, "polymer_layers_cost_info.json"), "r") as file:
            PL_costing_params = json.load(file)

    # accounts 3.x are for water systems
    # all scale with feedwater flowrate in lb/hr, except 3.2 water makeup/pretreating
    # which scales with raw water withdrawl in gal/min

    feedwater_and_boiler_plant_accounts = ["3.1", "3.3", "3.5"]
    blk.h2o_mixer.feedwater_flow = Var(initialize=390410.122, units=pyunits.lb/pyunits.hr)
    blk.h2o_mixer.feedwater_flow_constraint = Constraint(
        expr=(
            blk.h2o_mixer.feedwater_flow ==
            pyunits.convert(
                blk.h2o_mixer.feed_inlet.flow_mol[0] * blk.o2_prop.H2O.mw,
                to_units=pyunits.lb/pyunits.hr
                )
            )
        )

    blk.h2o_mixer.costing = UnitModelCostingBlock(
        flowsheet_costing_block=blk.costing,
        costing_method=QGESSCostingData.get_PP_costing,
        costing_method_arguments={
            "cost_accounts": feedwater_and_boiler_plant_accounts,
            "scaled_param": blk.h2o_mixer.feedwater_flow,
            "tech": 8,
            "ccs": "B",
            "additional_costing_params": PL_costing_params,
            "CE_index_year": cost_year,
   #         "multiply_project_conting": False,
        },
    )

    feedwater_makeup_and_pretreating_accounts = ["3.2",]
    blk.lean_fa_mixer.makeup_water_raw_withdrawal = Var(initialize=1410.0, units=pyunits.gal/pyunits.min)
    blk.lean_fa_mixer.h2o_density = Param(initialize=1000, units=pyunits.kg/pyunits.m**3, mutable=True)
    blk.lean_fa_mixer.makeup_water_raw_withdrawal_constraint = Constraint(
        expr=(
            blk.lean_fa_mixer.makeup_water_raw_withdrawal ==
            pyunits.convert(
                blk.lean_fa_mixer.makeup_inlet.flow_mol[0]
                * blk.o2_prop.H2O.mw / blk.lean_fa_mixer.h2o_density,
                to_units=pyunits.gal/pyunits.min
                )
            )
        )

    blk.lean_fa_mixer.costing = UnitModelCostingBlock(
        flowsheet_costing_block=blk.costing,
        costing_method=QGESSCostingData.get_PP_costing,
        costing_method_arguments={
            "cost_accounts": feedwater_makeup_and_pretreating_accounts,
            "scaled_param": blk.lean_fa_mixer.makeup_water_raw_withdrawal,
            "tech": 8,
            "ccs": "B",
            "additional_costing_params": PL_costing_params,
            "CE_index_year": cost_year,
            "multiply_project_conting": False,
        },
    )


    # Need to use parameter == feedwate total flow -10152024
    water_waste_treating_accounts = ["3.7",]
    blk.o2_separator.total_wastewater_flow = Var(
        initialize=281861.711,
        units=pyunits.gal/pyunits.min
        )
    #blk.o2_separator.total_wastewater_flow_constraint = Constraint(
    #    expr=(
    #        blk.o2_separator.total_wastewater_flow ==
    #        pyunits.convert(
    #            (blk.h2o_mixer.recycle_inlet.flow_mol[0] + blk.lean_fa_mixer.recycle_inlet.flow_mol[0])
    #            * blk.o2_prop.H2O.mw / blk.lean_fa_mixer.h2o_density,
    #            to_units=pyunits.gal/pyunits.min
    #            )
    #        )
    #    )
    blk.o2_separator.total_wastewater_flow_constraint = Constraint(
        expr=(
            blk.o2_separator.total_wastewater_flow ==
            pyunits.convert(
                (blk.h2o_mixer.feed_inlet.flow_mol[0] + blk.lean_fa_mixer.makeup_inlet.flow_mol[0])
                * blk.o2_prop.H2O.mw / blk.lean_fa_mixer.h2o_density,
                to_units=pyunits.gal/pyunits.min
                )
            )
        )

    blk.o2_separator.costing = UnitModelCostingBlock(
        flowsheet_costing_block=blk.costing,
        costing_method=QGESSCostingData.get_PP_costing,
        costing_method_arguments={
            "cost_accounts": water_waste_treating_accounts,
            "scaled_param": blk.o2_separator.total_wastewater_flow,
            "tech": 8,
            "ccs": "B",
            "additional_costing_params": PL_costing_params,
            "CE_index_year": cost_year,
            "multiply_project_conting": False,
        },
    )

    # accounts 4.x are for polymer layer separation and purification systems

    # electrolyzer

    electrolyzer_accounts = ["4.1",]
    blk.ez_module.electrode_area = Var(
        initialize=5510.428217241799,
        units=pyunits.m**2
        )
    blk.ez_module.electrode_area_constraint = Constraint(
        expr=(
            blk.ez_module.electrode_area ==
            pyunits.convert(
                blk.ez_module.number_cells *
                blk.ez_module.electrochemical_cell.length_y *
                blk.ez_module.electrochemical_cell.length_z,
                to_units=pyunits.m**2
                )
            )
        )

    blk.ez_module.costing = UnitModelCostingBlock(
        flowsheet_costing_block=blk.costing,
        costing_method=QGESSCostingData.get_PP_costing,
        costing_method_arguments={
            "cost_accounts": electrolyzer_accounts,
            "scaled_param": blk.ez_module.electrode_area,
            "tech": 8,
            "ccs": "B",
            "additional_costing_params": PL_costing_params,
            "CE_index_year": cost_year,
            "multiply_project_conting": False,
        },
    )

    # distillation

    if distillation_type == "reactive":
        distillation_accounts = ["4.2a",]
    elif distillation_type == "extractive":
        distillation_accounts = ["4.2b"]
    else:
        raise TypeError(f"{distillation_type} is not a valid distillation type. "
                        "Please set the distillation type to 'reactive' or 'extractive'.")

    blk.acid_separator.formic_acid_production = Var(
        initialize=1500.0,
        units=pyunits.kg/pyunits.hr
        )
    blk.acid_separator.feedwater_flow_constraint = Constraint(
        expr=(
            blk.acid_separator.formic_acid_production ==
            pyunits.convert(
                blk.acid_separator.acid_outlet.flow_mol[0] *blk.acid_separator.acid_outlet.mole_frac_comp[0,'CH2O2'] * blk.fa_prop.CH2O2.mw,
                to_units=pyunits.kg/pyunits.hr
                ) +
            pyunits.convert(
                blk.acid_separator.acid_outlet.flow_mol[0] *blk.acid_separator.acid_outlet.mole_frac_comp[0,'H2O'] * blk.fa_prop.H2O.mw,
                to_units=pyunits.kg/pyunits.hr
                )
            )
        )

    blk.acid_separator.costing = UnitModelCostingBlock(
        flowsheet_costing_block=blk.costing,
        costing_method=QGESSCostingData.get_PP_costing,
        costing_method_arguments={
            "cost_accounts": distillation_accounts,
            "scaled_param": blk.acid_separator.formic_acid_production,
            "tech": 8,
            "ccs": "B",
            "additional_costing_params": PL_costing_params,
            "CE_index_year": cost_year,
            "multiply_project_conting": False,
        },
    )

    # pressure swing absorber

    pressure_swing_absorber_accounts = ["4.3",]

    blk.co2_separator.costing = UnitModelCostingBlock(
        flowsheet_costing_block=blk.costing,
        costing_method=QGESSCostingData.get_PP_costing,
        costing_method_arguments={
            "cost_accounts": pressure_swing_absorber_accounts,
            "scaled_param": blk.co2_separator.mixed_state[0].flow_mol,
            "tech": 8,
            "ccs": "B",
            "additional_costing_params": PL_costing_params,
            "CE_index_year": cost_year,
            "multiply_project_conting": False,
        },
    )

    # CO2 recycle compressor

    recycle_compressor_accounts = ["4.5",]

    blk.co2_feed_compressor.costing = UnitModelCostingBlock(
        flowsheet_costing_block=blk.costing,
        costing_method=QGESSCostingData.get_PP_costing,
        costing_method_arguments={
            "cost_accounts": recycle_compressor_accounts,
            "scaled_param": blk.co2_feed_compressor.control_volume.work[0],
            "tech": 8,
            "ccs": "B",
            "additional_costing_params": PL_costing_params,
            "CE_index_year": cost_year,
            "multiply_project_conting": False,
        },
    )

    # CO2 capture membrane

    capture_membrane_accounts = ["4.6",]

    blk.membrane.costing = UnitModelCostingBlock(
        flowsheet_costing_block=blk.costing,
        costing_method=QGESSCostingData.get_PP_costing,
        costing_method_arguments={
            "cost_accounts": capture_membrane_accounts,
            "scaled_param": blk.membrane.area,
            "tech": 8,
            "ccs": "B",
            "additional_costing_params": PL_costing_params,
            "CE_index_year": cost_year,
            "multiply_project_conting": False,
        },
    )

    # accounts 11.x are balance-of-plant systems (motors, cables)
    # accounts 12.x are instrumentation and controls
    # accounts 13.x are site-related items
    # accounts 14.x are water circulation systems
    # all accounts in all three groups scale with auxiliary load in kW

    bop_instrumentation_site_and_water_circulation_accounts = [
        "11.2",
        "11.3",
        "11.4",
        "11.5",
        "12.8",
        "12.9",
        "13.1",
        "13.2",
        "13.3",
        "14.5",
        ]
    # no bop block on flowsheet, so create new UnitModelBlock()
    blk.bop_instrumentation_site_and_water_circulation = UnitModelBlock()
    blk.bop_instrumentation_site_and_water_circulation.auxiliary_load = Var(
        initialize=(
            pyunits.convert(
                blk.acid_separator.formic_acid_production,
                to_units=pyunits.kg/pyunits.hr
                ) / ( 6450* pyunits.kg/pyunits.hr)  # 1500  reference from report case 4
            * (58865) * pyunits.kW  # reference from report case 4 #15910.524511887934
            ),
        units=pyunits.kW
        )
    blk.bop_instrumentation_site_and_water_circulation.auxiliary_load_constraint = Constraint(
        expr=(
            blk.bop_instrumentation_site_and_water_circulation.auxiliary_load == pyunits.convert(
                blk.acid_separator.formic_acid_production,
                to_units=pyunits.kg/pyunits.hr
                ) / ( 6450* pyunits.kg/pyunits.hr)  # 1500  reference from report case 4
            * (58865) * pyunits.kW  # reference from report case #415910.524511887934
            )
        )

    blk.bop_instrumentation_site_and_water_circulation.costing = UnitModelCostingBlock(
        flowsheet_costing_block=blk.costing,
        costing_method=QGESSCostingData.get_PP_costing,
        costing_method_arguments={
            "cost_accounts": bop_instrumentation_site_and_water_circulation_accounts,
            "scaled_param": blk.bop_instrumentation_site_and_water_circulation.auxiliary_load,
            "tech": 8,
            "ccs": "B",
            "additional_costing_params": PL_costing_params,
            "CE_index_year": cost_year,
            "multiply_project_conting": False,
        },
    )

    # use resource rates - values from S. McNaul, "Screening Techno-economic
    # Analysis of NETL Reactive Capture Technology," National Energy Technology
    # Laboratory, Pittsburgh, September 30, 2022. Exhibit B-8 as initial values
    # report case 4 rate = X units per day * 85% capacity_factor

    blk.process_water = Var(blk.time, initialize=24.3*0.85, units=pyunits.gal/pyunits.day)
    # process water = feed water + makeup water
    blk.process_water_constraint = Constraint(
        expr=(
            blk.process_water[0] == pyunits.convert(
                (blk.h2o_mixer.feed_inlet.flow_mol[0] + blk.lean_fa_mixer.makeup_inlet.flow_mol[0])
                * blk.o2_prop.H2O.mw / blk.lean_fa_mixer.h2o_density,
                to_units=pyunits.gal/pyunits.day
                )
            )
        )

    blk.makeup_water_treatment_chemicals = Var(blk.time, initialize=0.3*0.85, units=pyunits.ton/pyunits.day)
    # 0.3 ton per 1000 gallons process water
    blk.makeup_water_treatment_chemicals_constraint = Constraint(
        expr=(
            blk.makeup_water_treatment_chemicals[0] ==
            (0.3 * pyunits.ton)/(1000 * pyunits.gal) * blk.process_water[0]
            )
        )

    blk.PSA_adsorbent_initial_volume = Var(blk.time, initialize=1392.8, units=pyunits.ft**3)
    # NETL report case 4 reference 1392.8 ft3 initial volume for 562 kmol/h gas feed

    blk.PSA_adsorbent_initial_volume_constraint = Constraint(
        expr=(
            blk.PSA_adsorbent_initial_volume[0] == pyunits.convert(
            1392.8 * pyunits.ft**3 * (blk.co2_separator.mixed_state[0].flow_mol)/(562000 * pyunits.mol/pyunits.h),
            to_units=pyunits.ft**3
            )
            )
        )

    blk.PSA_adsorbent_flowrate_basis = Var(blk.time, initialize=0.2*0.85, units=pyunits.ft**3/pyunits.day)
    # daily replacement rate is 0.015% of total initial volume
    blk.PSA_adsorbent_flowrate_basis_constraint = Constraint(
        expr=(
            blk.PSA_adsorbent_flowrate_basis[0] ==
            0.015/100 * blk.PSA_adsorbent_initial_volume[0] / pyunits.day
            )
        )

    blk.membrane_replacement = Var(blk.time, initialize=15.1*0.85, units=pyunits.m**2/pyunits.day)
    # daily replacement rate equivalent of replacing entire membrane every 5 years
    blk.membrane_replacement_constraint = Constraint(
        expr=(
            blk.membrane_replacement[0] == pyunits.convert(
                blk.membrane.area / (5 * pyunits.year),
                to_units=pyunits.m**2/pyunits.day
                )
            )
        )

    blk.PSA_adsorbent_waste_disposal = Var(blk.time, initialize=0.2*0.85, units=pyunits.ft**3/pyunits.day)
    # equivalent to adsorbent replacement rate, this is the adsorbent that was replaced
    blk.PSA_adsorbent_waste_disposal_constraint = Constraint(
        expr=(
            blk.PSA_adsorbent_waste_disposal[0] ==
            blk.PSA_adsorbent_flowrate_basis[0]
            )
        )

    resources = [
        "process_water",
        "makeup_water_treatment_chemicals",
        "PSA_adsorbent",
        "membrane_replacement",
        "PSA_adsorbent_waste_disposal",
        ]

    rates = [
        blk.process_water,
        blk.makeup_water_treatment_chemicals,
        blk.PSA_adsorbent_flowrate_basis,
        blk.membrane_replacement,
        blk.PSA_adsorbent_waste_disposal,
        ]

    if distillation_type == "extractive":
        # capital cost already includes reboilers, condensers, etc so we don't
        # need to any additional capital equipment blocks here

        # initial values estimated from duty required per production rate from
        # X. Ge, R. Zhang, P. Liu, B. Liu, B. Liu, Optimization and control of
        # extractive distillation for formic acid-water separation with maximum-boiling
        # azeotrope, Computers & Chemical Engineering, Vol. 169, 2023. Table 3.
        # for 3233.38 kg/hr formic acid produced, distillation column uses -5340 kW
        # cooling water for the condenser and 6943 kW HP heating steam for the reboiler,
        # recovery columns uses -1054 kW chilled water for the condenser and 1286 kW HP
        # heating steam for the reboiler, and heat exchanger uses -1240 kW cooling water.
        blk.cooling_water = Var(
            blk.time,
            initialize=(
                pyunits.convert(
                    blk.acid_separator.formic_acid_production,
                    to_units=pyunits.kg/pyunits.hr
                    ) / (3233.38 * pyunits.kg/pyunits.hr)
                * (5340 + 1240) * pyunits.kW
                ),
            units=pyunits.kW
            )
        blk.cooling_water_constraint = Constraint(
            expr=(
                blk.cooling_water[0] == pyunits.convert(
                    blk.acid_separator.edc_condenser_duty[0]
                    + blk.acid_separator.sulfo_cooler_duty[0],
                    to_units=pyunits.kW
                    )
                )
            )

        blk.chilled_water = Var(
            blk.time,
            initialize=(
                pyunits.convert(
                    blk.acid_separator.formic_acid_production,
                    to_units=pyunits.kg/pyunits.hr
                    ) / (3233.38 * pyunits.kg/pyunits.hr)
                * (1054) * pyunits.kW
                ),
            units=pyunits.kW
            )
        blk.chilled_water_constraint = Constraint(
            expr=(
                blk.chilled_water[0] == pyunits.convert(
                    blk.acid_separator.erc_condenser_duty[0],
                    to_units=pyunits.kW
                    )
                )
            )

        blk.hp_heating_steam = Var(
            blk.time,
            initialize=(
                pyunits.convert(
                    blk.acid_separator.formic_acid_production,
                    to_units=pyunits.kg/pyunits.hr
                    ) / (3233.38 * pyunits.kg/pyunits.hr)
                * (6943 + 1286) * pyunits.kW
                ),
            units=pyunits.kW
            )
        blk.hp_heating_steam_constraint = Constraint(
            expr=(
                blk.hp_heating_steam[0] == pyunits.convert(
                    blk.acid_separator.edc_reboiler_duty[0]
                    + blk.acid_separator.erc_reboiler_duty[0],
                    to_units=pyunits.kW
                    )
                )
            )

        blk.sulfolane_entrainer = Var(
            blk.time,
            initialize=(
                pyunits.convert(
                    blk.acid_separator.formic_acid_production,
                    to_units=pyunits.kg/pyunits.hr
                    ) / (3233.38 * pyunits.kg/pyunits.hr)
                * (14.51) * pyunits.kg/pyunits.hr
                ),
            units=pyunits.kg/pyunits.hr
            )
        blk.sulfolane_entrainer_constraint = Constraint(
            expr=(
                blk.sulfolane_entrainer[0] == pyunits.convert(
                    blk.acid_separator.formic_acid_production,
                    to_units=pyunits.kg/pyunits.hr
                    ) / (3233.38 * pyunits.kg/pyunits.hr)
                * (14.51) * pyunits.kg/pyunits.hr
                )
            )

        resources.append("cooling_water")
        resources.append("chilled_water")
        resources.append("hp_heating_steam")
        resources.append("sulfolane_entrainer")
        rates.append(blk.cooling_water)
        rates.append(blk.chilled_water)
        rates.append(blk.hp_heating_steam)
        rates.append(blk.sulfolane_entrainer)

    elif distillation_type == "reactive":
        # reactive case has electric boiler for heat duty
        # boiler was not included in distillation equipment cost, so add it here
        # as above, scale reference value with FA production rate

        electric_boiler_accounts = ["4.4",]
        blk.electric_boiler = UnitModelBlock()
        blk.electric_boiler.auxiliary_load = Var(
            initialize=(
                pyunits.convert(
                    blk.acid_separator.formic_acid_production,
                    to_units=pyunits.kg/pyunits.hr
                    ) / ( 6450* pyunits.kg/pyunits.hr)  # 1500 reference from report case 4
                * (58865) * pyunits.kW  # reference from report case 4 19510.0
                ),
            units=pyunits.kW
            )
        blk.electric_boiler_auxiliary_load_constraint = Constraint(
            expr=(
                blk.electric_boiler.auxiliary_load == pyunits.convert(
                    blk.acid_separator.formic_acid_production,
                    to_units=pyunits.kg/pyunits.hr
                    ) / ( 6450* pyunits.kg/pyunits.hr)  # 1500 reference from report case 4
                * (58865) * pyunits.kW  # reference from report case 4 #19510.0
                )
            )

        blk.electric_boiler.costing = UnitModelCostingBlock(
            flowsheet_costing_block=blk.costing,
            costing_method=QGESSCostingData.get_PP_costing,
            costing_method_arguments={
                "cost_accounts": electric_boiler_accounts,
                "scaled_param": blk.electric_boiler.auxiliary_load,
                "tech": 8,
                "ccs": "B",
                "additional_costing_params": PL_costing_params,
                "CE_index_year": cost_year,
                "multiply_project_conting": False,
            },
        )

        # reactive distillation uses amine, extractive distillation uses sulfolane
        blk.amine_entrainer = Var(
            blk.time,
            initialize=(
                pyunits.convert(
                    blk.acid_separator.formic_acid_production,
                    to_units=pyunits.kg/pyunits.hr
                    ) / (6450 * pyunits.kg/pyunits.hr)  # 1500  reference from report case 4
                * (9.6*0.85) * pyunits.kg/pyunits.day  # reference from report case 4
                ),
            units=pyunits.kg/pyunits.day
            )
        blk.amine_entrainer_constraint = Constraint(
            expr=(
                blk.amine_entrainer[0] == pyunits.convert(
                    blk.acid_separator.formic_acid_production,
                    to_units=pyunits.kg/pyunits.hr
                    ) / ( 6450*pyunits.kg/pyunits.hr)  #1500 reference from report case 4
                * (9.6*0.85) * pyunits.kg/pyunits.day  # reference from report case 4
                )
            )

        resources.append("amine_entrainer")
        rates.append(blk.amine_entrainer)

    # add electricity rate, initial value is from report case 4
    blk.electricity = Var(blk.time, initialize=1412.8*0.85, units=pyunits.MWh/pyunits.day)
    # electrical power from electrolyzer, recycle compressor, electric boiler, bop/circulation system
    blk.electricity_constraint = Constraint(
        expr=(
            blk.electricity[0] ==
            # electrolyzer power = current * voltage
            pyunits.convert(abs(blk.ez_module.total_current[0] * blk.ez_module.potential_cell[0]), to_units=pyunits.MWh/pyunits.day)
            # recycle compressor power = auxiliary load
            + pyunits.convert(blk.co2_feed_compressor.control_volume.work[0], to_units=pyunits.MWh/pyunits.day)
            # electric boiler power = auxiliary load; conditional because extractive case doesn't have electric boiler
            + (pyunits.convert(blk.electric_boiler.auxiliary_load, to_units=pyunits.MWh/pyunits.day)
                if hasattr(blk, "electric_boiler") else 0 * pyunits.MWh/pyunits.day)
            # bop/circulation power = auxiliary load
            +pyunits.convert(blk.bop_instrumentation_site_and_water_circulation.auxiliary_load, to_units=pyunits.MWh/pyunits.day)
            # remove double count EZ power
            -pyunits.convert(
                pyunits.convert(
                    blk.acid_separator.formic_acid_production,
                    to_units=pyunits.kg/pyunits.hr
                    ) / ( 6450* pyunits.kg/pyunits.hr)  # 1500  reference from report case 4
                * (41000) * pyunits.kW  # reference from report case 4 #15910.524511887934
                , to_units=pyunits.MWh/pyunits.day)
            )
        )
    resources.append("electricity")
    rates.append(blk.electricity)

    prices = {
        # from S. McNaul, "Screening Techno-economic Analysis of NETL Reactive
        # Capture Technology," National Energy Technology Laboratory, Pittsburgh,
        # September 30, 2022. Exhibit B-8
        "process_water": 1.9e-3 * pyunits.USD_2018_Dec / pyunits.gal,
        "makeup_water_treatment_chemicals": 550 * pyunits.USD_2018_Dec / pyunits.ton,
        "PSA_adsorbent": 150 * pyunits.USD_2018_Dec / pyunits.ft**3,
        "amine_entrainer": 861.1 * pyunits.USD_2018_Dec / pyunits.kg,
        "electricity": 71.7 * pyunits.USD_2018_Dec / pyunits.MWh,
        "membrane_replacement": 15.1 * pyunits.USD_2018_Dec / pyunits.m**2,
        "PSA_adsorbent_waste_disposal": 1.5 * pyunits.USD_2018_Dec / pyunits.ft**3,
        "nonharzardous_waste_disposal": 38 * pyunits.USD_2018_Dec / pyunits.ton,
        # from X. Ge, R. Zhang, P. Liu, B. Liu, B. Liu, Optimization and control of
        # extractive distillation for formic acid-water separation with maximum-boiling
        # azeotrope, Computers & Chemical Engineering, Vol. 169, 2023. Table 2.
        "cooling_water": 0.354 * pyunits.USD_2016 / pyunits.GJ,
        "chilled_water": 4.42 * pyunits.USD_2016 / pyunits.GJ,
        "lp_heating_steam": 7.78 * pyunits.USD_2016 / pyunits.GJ,
        "mp_heating_steam": 8.22 * pyunits.USD_2016 / pyunits.GJ,
        "hp_heating_steam": 9.83 * pyunits.USD_2016 / pyunits.GJ,
        # from Zaiz, Toufik & Lanez, Hafnaoui. (2013). ASPEN HYSYS SIMULATION AND
        # COMPARISON BETWEEN ORGANIC SOLVENTS (SULFOLANE AND DMSO) USED FOR BENZENE
        # EXTRACTION. International Journal of Chemical and Petroleum Sciences. 2.
        # 10-19. Section 7.1b on page 18.
        "sulfolane_entrainer": 3700 * pyunits.USD_2013 / pyunits.ton,

        }

    blk.costing.land_cost_expression = Expression(expr=0.3*pyunits.MUSD_2018_Dec)  # straight land leasing cost

    blk.costing.initial_fill_expression = Expression(
        expr=blk.PSA_adsorbent_initial_volume[0] * prices["PSA_adsorbent"]
        )  # PSA adsorbent

    blk.costing.transport_cost_per_tonne_CO2 = Expression(
        expr=10*pyunits.USD_2018_Dec/pyunits.tonne
        )  # $/tonne CO2 transported

    blk.costing.tonne_CO2_capture = Var(
        initialize=pyunits.convert(
            6225 * pyunits.kg/pyunits.hr,
            to_units=pyunits.tonne/pyunits.year
            ),
        units=pyunits.tonne/pyunits.year
        )
    blk.costing.tonne_CO2_capture_constraint = Constraint(
        expr=(
            blk.costing.tonne_CO2_capture==pyunits.convert(
                (
                    blk.membrane.feed_side_inlet.flow_mol[0]
                    * blk.membrane.feed_side_inlet.mole_frac_comp[0, "CO2"]
                    * blk.flue_prop.CO2.mw
                    )
                - (
                    blk.co2_separator.n2_outlet.flow_mol[0]
                    * blk.co2_separator.n2_outlet.mole_frac_comp[0, "CO2"]
                    * blk.flue_prop.CO2.mw
                    ),
                to_units=pyunits.tonne/pyunits.year
                )
            )
        )

    blk.costing.annual_production_rate = Var(
        initialize=value(
            pyunits.convert(
                6450 * pyunits.kg/pyunits.hr,
                to_units=pyunits.tonne/pyunits.year
                )
            ),
        units=pyunits.tonne/pyunits.year
        )
    blk.costing.annual_production_rate_constraint = Constraint(
        expr=(
            blk.costing.annual_production_rate==pyunits.convert(
                blk.acid_separator.formic_acid_production,
                to_units=pyunits.tonne/pyunits.year
                )
            )
        )

    blk.costing.build_process_costs(
        capacity_factor=capacity_factor,
        fixed_OM=True,
        labor_rate=38.50,
        labor_burden=30,
        operators_per_shift=5,
        tech=8,  # technology code for polymer layers economic parameters
        land_cost=blk.costing.land_cost_expression,
        variable_OM=True,
        resources=resources,
        rates=rates,
        prices=prices,
        waste=["PSA_adsorbent_waste_disposal"],
        # process water, electricity, PSA adsorbent waste, and utilites for distillation
        # are not considered chemicals; everything else has chemical and storage costs
        chemicals=[
            i for i in resources if i not in
            ["process_water", "electricity", "PSA_adsorbent_waste_disposal",
              "cooling_water", "chilled_water", "hp_heating_steam",]
            ],
        chemicals_inventory=[
            i for i in resources if i not in
            ["process_water", "electricity", "PSA_adsorbent_waste_disposal",
              "cooling_water", "chilled_water", "hp_heating_steam",]
            ],
        initial_fills=blk.costing.initial_fill_expression,  # initial fill cost for PSA adsorbent (chemical cost for tech=8)
        transport_cost_per_tonne_CO2=blk.costing.transport_cost_per_tonne_CO2,  # for CO2 from and to the pipeline
        tonne_CO2_capture=blk.costing.tonne_CO2_capture,    # flow of CO2 captured
        annual_production_rate=blk.costing.annual_production_rate,
        CE_index_year="2018_Dec"
        )




def report_costing_results(blk):
    QGESSCostingData.report(blk.costing)
    QGESSCostingData.display_total_plant_costs(blk.costing)
    print()
    print("Owner's Costs Breakdown")
    print("=======================")
    print()

    print("6 months All Labor [$/1,000]: ", 1e3*value(blk.costing.six_month_labor))
    print("1-month Maintenance Materials [$/1,000]: ", 1e3*value(blk.costing.maintenance_material_cost/12/blk.costing.capacity_factor))
    print("1-month Non-Fuel Consumables [$/1,000]: ", 1e3*value(blk.costing.non_fuel_and_waste_OC))
    print("1-month Waste Disposal [$/1,000]: ", 1e3*value(blk.costing.waste_cost_OC))
    print("No fuel or feedstock in this case")
    print("60-Day Supply of Chemical Consumables [$/1,000]: ", 1e3*value(blk.costing.chemicals_inventory_cost_OC))
    print("0.5% TPC for Spare Parts + Other Owner's + Financing + 2% TPC [$/1,000]: ", 1e3*value(blk.costing.pct_TPC * blk.costing.total_TPC))
    print("Initial Cost for Catalyst and Chemicals [$/1,000]: ", 1e3*value(blk.costing.chemicals_cost_OC))
    print("Land [$/1,000]: ", 1e3*value(blk.costing.land_cost))

    print()
    print("Cost Summary")
    print("=======================")
    print("Capital LCOP [$/kg formic acid]: ", value(blk.costing.annualized_cost*1e6/(blk.costing.annual_production_rate*1e3*blk.costing.capacity_factor)))
    print("Fixed O&M LCOP [$/kg formic acid]: ", value(blk.costing.total_fixed_OM_cost*1e6/(blk.costing.annual_production_rate*1e3*blk.costing.capacity_factor)))
    print("Variable O&M LCOP [$/kg formic acid]: ", value(blk.costing.total_variable_OM_cost[0]*1e6/(blk.costing.annual_production_rate*1e3*blk.costing.capacity_factor)))
    print("Total LCOP [$/kg formic acid]: ", value(blk.costing.cost_of_production*1e6))


