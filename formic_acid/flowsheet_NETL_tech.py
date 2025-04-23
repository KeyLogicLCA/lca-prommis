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
import idaes.logger as idaeslog

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

from costing_utility import (build_costing, report_costing_results)

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

def main_steady_state(optimize=False, dev_mode = True ):
    m = ConcreteModel("NETL_reactive_capture_plant")
    m.fs = FlowsheetBlock(dynamic=False, time_set=[0], time_units=pyunits.s)
    # Anode side/O2 side property, contains O2 and H2O
    m.fs.o2_prop = GenericParameterBlock(
        **get_prop(["O2", "H2O"], ["Vap"], eos=EosType.IDEAL),
        doc="O2 side sweep gas property parameters",
        )
    # Cathode side/flue gas side property, contains N2 and CO2
    m.fs.flue_prop = GenericParameterBlock(
        **get_prop(["N2", "CO2", "O2", "D2O", "Ar"], ["Vap"], eos=EosType.IDEAL),
        doc="flue gas side gas property parameters",
        )
    # Formic acid channel property, contains CH2O2 and H2O
    m.fs.fa_prop = GenericParameterBlock(
            **get_prop(["H2O", "CH2O2"], ["Vap"], eos=EosType.IDEAL),
            doc="acid side gas property parameters",
        )

    # declare a membrane unit
    m.fs.membrane = Membrane1D(
        finite_elements = 1,
        dynamic=False,
        sweep_flow = False,
        flow_type = MembraneFlowPattern.COUNTERCURRENT,
        property_package = m.fs.flue_prop,
        )

    # membrane model inputs
    m.fs.membrane.permeance[:,:,'CO2'].fix(4000)
    m.fs.membrane.permeance[:,:,'N2'].fix(4000/11)
    m.fs.membrane.permeance[:,:,'O2'].fix(4000/5)
    m.fs.membrane.permeance[:,:,'D2O'].fix(4000/0.09)
    m.fs.membrane.permeance[:,:,'Ar'].fix(4000/5)
    m.fs.membrane.area.fix(2e8)# 2e8 6.4e8
    m.fs.membrane.length.fix(10)
    m.fs.membrane.sweep_side_outlet.pressure[0].fix(100000)  # 51325Pa

    m.fs.membrane.feed_side_inlet.flow_mol[0].fix(13000)  #17500 1750 5000 10000mol/s
    m.fs.membrane.feed_side_inlet.temperature[0].fix(300)  # K
    m.fs.membrane.feed_side_inlet.pressure[0].fix(200000) # 250000
    m.fs.membrane.feed_side_inlet.mole_frac_comp[0, "N2"].fix(0.7442)
    m.fs.membrane.feed_side_inlet.mole_frac_comp[0, "CO2"].fix(0.0391)
    m.fs.membrane.feed_side_inlet.mole_frac_comp[0, "O2"].fix(0.1238)
    m.fs.membrane.feed_side_inlet.mole_frac_comp[0, "D2O"].fix(0.0841)
    m.fs.membrane.feed_side_inlet.mole_frac_comp[0, "Ar"].fix(0.0089)

    if dev_mode:
        initializer = BlockTriangularizationInitializer(constraint_tolerance=2e-5)
        initializer.initialize(m.fs.membrane)

    # declare a compressor unit
    m.fs.co2_feed_compressor = Compressor(
        property_package = m.fs.flue_prop,
        )

    # compressor model inputs
    m.fs.co2_feed_compressor.deltaP.fix(1)
    m.fs.co2_feed_compressor.efficiency_isentropic.fix(0.9)

    if dev_mode:
        m.fs.membrane_to_compressor = Arc(
            source=m.fs.membrane.sweep_side_outlet, destination=m.fs.co2_feed_compressor.inlet
        )

        TransformationFactory("network.expand_arcs").apply_to(m.fs)
        propagate_state(m.fs.membrane_to_compressor)
        initializer.initialize(m.fs.co2_feed_compressor)


    # declare a mixer
    m.fs.co2_mixer = Mixer(
        property_package = m.fs.flue_prop,
        inlet_list = ["compressor_inlet", "recycle_inlet"],
        momentum_mixing_type = MomentumMixingType.none,
    )


    # co2_mixer model inputs
    m.fs.co2_mixer.recycle_inlet.flow_mol[0].value =52.5  #150 300
    m.fs.co2_mixer.recycle_inlet.pressure[0].value = 100000
    m.fs.co2_mixer.recycle_inlet.temperature[0].value = 336
    m.fs.co2_mixer.recycle_inlet.mole_frac_comp[0,"CO2"].value = 0.295
    m.fs.co2_mixer.recycle_inlet.mole_frac_comp[0,"D2O"].value = 0.695
    m.fs.co2_mixer.recycle_inlet.mole_frac_comp[0,"N2"].value = 0.01
    m.fs.co2_mixer.recycle_inlet.mole_frac_comp[0,"O2"].value = 1e-5
    m.fs.co2_mixer.recycle_inlet.mole_frac_comp[0,"Ar"].value = 1e-5

    if dev_mode:
        m.fs.co2_mixer.recycle_inlet.fix()
        m.fs.compressor_to_co2_mixer = Arc(
            source=m.fs.co2_feed_compressor.outlet, destination=m.fs.co2_mixer.compressor_inlet
        )

        TransformationFactory("network.expand_arcs").apply_to(m.fs)
        propagate_state(m.fs.compressor_to_co2_mixer)
        m.fs.co2_mixer.initialize(outlvl=idaeslog.INFO_LOW)
        m.fs.co2_mixer.display()
        print('dof=',degrees_of_freedom(m))

    # Constraints for mixer outlet pressure
    @m.fs.co2_mixer.Constraint(m.fs.config.time, doc="Outlet pressure identical to compressor outlet pressure")
    def co2_mixer_pressure_eqn(b, t):
        return 1e-5*b.compressor_inlet.pressure[t] == 1e-5*b.outlet.pressure[t]


    # declare a electrolyzer model
    zfaces = np.linspace(0, 1, 11).tolist()
    xfaces_electrode = [0.0, 1.0]
    xfaces_electrolyte = [0.0, 1.0]

    ez_cell_config = {
        "has_holdup": True,
        "dynamic": False,
        "has_gas_holdup": False,
        "control_volume_zfaces": zfaces,
        "control_volume_xfaces_fuel_electrode": xfaces_electrode,
        "fuel_component_list": ["CO2", "N2", "O2", "D2O", "Ar"],
        "acid_component_list": ["H2O", "CH2O2"],
        "combined_component_list": ["CO2", "N2", "O2", "D2O", "Ar", "H2O", "CH2O2"],
        "fuel_triple_phase_boundary_stoich_dict": {"CH2O2": -0.5, "CO2": 0.5, "H^+":1.0, "e^-":1.0},
        "inert_fuel_species_triple_phase_boundary": ["N2","H2O","D2O","Ar","O2"],
        "flow_pattern": HeatExchangerFlowPattern.countercurrent,
        "include_temperature_x_thermo": True,
    }

    # include_interconnect
    ez_cell_config["flux_through_interconnect"] = True
    ez_cell_config["control_volume_xfaces_interconnect"] = [0.0, 1.0]

    ez_cell_config["oxygen_component_list"] = ["H2O","O2"]
    ez_cell_config["oxygen_triple_phase_boundary_stoich_dict"] = {
        "H2O": 0.5,
        "O2": -0.25,
        "H^+": -1.0,
        "e^-": -1.0
    }
    ez_cell_config["inert_oxygen_species_triple_phase_boundary"] = []
    ez_cell_config["control_volume_xfaces_oxygen_electrode"] = xfaces_electrode
    ez_cell_config["control_volume_xfaces_electrolyte"] = xfaces_electrolyte
    m.fs.ez_module = ElectrochemicalCellModuleSimple(
        dynamic=False,
        electrochemical_cell_config=ez_cell_config,
        fuel_property_package=m.fs.flue_prop,
        oxygen_property_package=m.fs.o2_prop,
        acid_property_package=m.fs.fa_prop,
    )

    # electrolyzer model inputs
    m.fs.ez_module.number_cells.fix(0.65e5)# 0.35e5  1e5 2e5
    ezc = m.fs.ez_module.electrochemical_cell
    ezc.fuel_channel.length_x.fix(873e-6)
    ezc.length_y.fix(0.2345)
    ezc.length_z.fix(0.2345)
    ezc.fuel_channel.heat_transfer_coefficient.fix(100)

    ezc.acid_channel.length_x.fix(873e-6)
    ezc.acid_channel.heat_transfer_coefficient.fix(100)

    ezc.oxygen_channel.length_x.fix(873e-6)
    ezc.oxygen_channel.heat_transfer_coefficient.fix(100)

    ezc.fuel_electrode.length_x.fix(1e-3)
    ezc.fuel_electrode.porosity.fix(0.326)
    ezc.fuel_electrode.tortuosity.fix(3)
    ezc.fuel_electrode.solid_heat_capacity.fix(595)
    ezc.fuel_electrode.solid_density.fix(7740.0)
    ezc.fuel_electrode.solid_thermal_conductivity.fix(6.23)
    ezc.fuel_electrode.resistivity_log_preexponential_factor\
        .fix(log(2.5e-5))
    ezc.fuel_electrode.resistivity_thermal_exponent_dividend.fix(0)

    ezc.oxygen_electrode.length_x.fix(40e-6)
    ezc.oxygen_electrode.porosity.fix(0.30717)
    ezc.oxygen_electrode.tortuosity.fix(3.0)
    ezc.oxygen_electrode.solid_heat_capacity.fix(142.3)
    ezc.oxygen_electrode.solid_density.fix(5300)
    ezc.oxygen_electrode.solid_thermal_conductivity.fix(2.0)
    ezc.oxygen_electrode.resistivity_log_preexponential_factor\
        .fix(log(7.8125e-05))
    ezc.oxygen_electrode.resistivity_thermal_exponent_dividend.fix(0)
    ezc.electrolyte.length_x.fix(10.5e-6)
    ezc.electrolyte.heat_capacity.fix(400)
    ezc.electrolyte.density.fix(6000)
    ezc.electrolyte.thermal_conductivity.fix(2.17)
    ezc.electrolyte.resistivity_log_preexponential_factor.fix(-9)
    ezc.electrolyte.resistivity_thermal_exponent_dividend.fix(1000) #original 8988 for SOEC

    ezc.fuel_triple_phase_boundary.exchange_current_log_preexponential_factor\
        .fix(22.5)
    ezc.fuel_triple_phase_boundary.exchange_current_activation_energy.fix(45.0e3)
    ezc.fuel_triple_phase_boundary.activation_potential_alpha1.fix(0.94)
    ezc.fuel_triple_phase_boundary.activation_potential_alpha2.fix(0.06)
    ezc.fuel_triple_phase_boundary.exchange_current_exponent_comp["CH2O2"].fix(0.5)
    ezc.fuel_triple_phase_boundary.exchange_current_exponent_comp["CO2"].fix(0.5)

    ezc.oxygen_triple_phase_boundary.exchange_current_log_preexponential_factor\
        .fix(25.5)
    ezc.oxygen_triple_phase_boundary.exchange_current_activation_energy.fix(47.0e3)
    ezc.oxygen_triple_phase_boundary.activation_potential_alpha1.fix(0.92)
    ezc.oxygen_triple_phase_boundary.activation_potential_alpha2.fix(0.08)

    ezc.oxygen_triple_phase_boundary.exchange_current_exponent_comp["O2"].fix(0.25)
    ezc.oxygen_triple_phase_boundary.exchange_current_exponent_comp["H2O"].fix(0.5)

    ezc.interconnect.length_x.fix(5e-3)
    ezc.interconnect.density.fix(7640)
    ezc.interconnect.heat_capacity.fix(948)
    ezc.interconnect.thermal_conductivity.fix(27)
    ezc.interconnect.resistivity_log_preexponential_factor.fix(log(110e-8))
    ezc.interconnect.resistivity_thermal_exponent_dividend.fix(0)

    ezc.oxygen_triple_phase_boundary.operating_temperature.fix(330)
    ezc.fuel_triple_phase_boundary.operating_temperature.fix(400)
    ezc.interconnect.temperature_wall_outside = 300

    m.fs.ez_module.potential_cell.fix(2.6)

    m.fs.ez_module.acid_inlet.flow_mol[0].value = 430
    m.fs.ez_module.acid_inlet.pressure[0].value = 100000
    m.fs.ez_module.acid_inlet.temperature[0].value = 370
    m.fs.ez_module.acid_inlet.mole_frac_comp[0,'CH2O2'].value = 0.01
    m.fs.ez_module.acid_inlet.mole_frac_comp[0, 'H2O'].value = 0.99
    if dev_mode:
        m.fs.ez_module.acid_inlet.fix()

    m.fs.ez_module.oxygen_inlet.flow_mol[0].value =455
    m.fs.ez_module.oxygen_inlet.pressure[0].value = 100000
    m.fs.ez_module.oxygen_inlet.temperature[0].value = 300
    m.fs.ez_module.oxygen_inlet.mole_frac_comp[0,'O2'].value = 0.0003
    m.fs.ez_module.oxygen_inlet.mole_frac_comp[0, 'H2O'].value = 0.9997

    if dev_mode:
        m.fs.ez_module.oxygen_inlet.fix()

        m.fs.co2_mixer_to_ez_module = Arc(
            source=m.fs.co2_mixer.outlet, destination=m.fs.ez_module.fuel_inlet
            )
        TransformationFactory("network.expand_arcs").apply_to(m.fs)
        propagate_state(m.fs.co2_mixer_to_ez_module)
        m.fs.ez_module.initialize_build(outlvl=idaeslog.INFO_LOW)


    # declare a PSA model
    m.fs.co2_separator = Separator(
        property_package=m.fs.flue_prop,
        split_basis=SplittingType.componentFlow,
        outlet_list=["co2_outlet","n2_outlet"],
        ideal_separation=False,
        has_phase_equilibrium=False,
    )

    # PSA model inputs
    m.fs.co2_separator.split_fraction[0, "co2_outlet", "CO2"].fix(0.99)
    m.fs.co2_separator.split_fraction[0, "co2_outlet", "D2O"].fix(0.99)
    m.fs.co2_separator.split_fraction[0, "co2_outlet", "N2"].fix(0.001)
    m.fs.co2_separator.split_fraction[0, "co2_outlet", "O2"].fix(1e-5)
    m.fs.co2_separator.split_fraction[0, "co2_outlet", "Ar"].fix(1e-5)

    # declare a ED model
    m.fs.acid_separator = Separator(
        property_package=m.fs.fa_prop,
        split_basis=SplittingType.componentFlow,
        outlet_list=["acid_outlet","water_outlet"],
        ideal_separation=False,
        has_phase_equilibrium=False,
    )

    if dev_mode:

        # ED model inputs
        m.fs.acid_separator.split_fraction[0, "acid_outlet", 'CH2O2'].fix(0.89)
        m.fs.acid_separator.split_fraction[0, "acid_outlet", 'H2O'].fix(0.015)


    # declare a o2 separator model
    m.fs.o2_separator = Separator(
        property_package=m.fs.o2_prop,
        split_basis=SplittingType.componentFlow,
        outlet_list=["o2_outlet","water_outlet"],
        ideal_separation=False,
        has_phase_equilibrium=False,
    )

    # o2 separator model inputs
    m.fs.o2_separator.split_fraction[0, "water_outlet", 'O2'].fix(0.01)
    m.fs.o2_separator.split_fraction[0, "water_outlet", 'H2O'].fix(0.99)


    if dev_mode:

        m.fs.ez_module_to_co2_separator = Arc(
            source=m.fs.ez_module.fuel_outlet, destination=m.fs.co2_separator.inlet
        )
        m.fs.ez_module_to_acid_separator = Arc(
            source=m.fs.ez_module.acid_outlet, destination=m.fs.acid_separator.inlet
        )
        m.fs.ez_module_to_o2_separator = Arc(
            source=m.fs.ez_module.oxygen_outlet, destination=m.fs.o2_separator.inlet
        )
        TransformationFactory("network.expand_arcs").apply_to(m.fs)

        propagate_state(m.fs.ez_module_to_co2_separator)
        m.fs.co2_separator.initialize(outlvl=idaeslog.INFO_LOW)
        propagate_state(m.fs.ez_module_to_acid_separator)
        m.fs.acid_separator.initialize(outlvl=idaeslog.INFO_LOW)
        propagate_state(m.fs.ez_module_to_o2_separator)
        m.fs.o2_separator.initialize(outlvl=idaeslog.INFO_LOW)


    # TO RECYCLE BY ADDING MIXERS

    m.fs.lean_fa_mixer = Mixer(
        property_package=m.fs.fa_prop,
        inlet_list = ["makeup_inlet", "recycle_inlet"],
        momentum_mixing_type = MomentumMixingType.none,
    )
    m.fs.h2o_mixer = Mixer(
        property_package=m.fs.o2_prop,
        inlet_list = ["feed_inlet", "recycle_inlet"],
        momentum_mixing_type = MomentumMixingType.none,
    )

    if dev_mode:

        m.fs.lean_fa_mixer.makeup_inlet.flow_mol.fix(10)
        m.fs.lean_fa_mixer.makeup_inlet.mole_frac_comp[0, 'CH2O2'].fix(0.0001)
        m.fs.lean_fa_mixer.makeup_inlet.mole_frac_comp[0, 'H2O'].fix(0.9999)
        m.fs.lean_fa_mixer.makeup_inlet.temperature.fix(300)
        m.fs.lean_fa_mixer.makeup_inlet.pressure.fix(101325)

        m.fs.h2o_mixer.feed_inlet.flow_mol.fix(10)
        m.fs.h2o_mixer.feed_inlet.mole_frac_comp[0, 'H2O'].fix(0.9999)
        m.fs.h2o_mixer.feed_inlet.mole_frac_comp[0, 'O2'].fix(0.0001)
        m.fs.h2o_mixer.feed_inlet.temperature.fix(300)
        m.fs.h2o_mixer.feed_inlet.pressure.fix(101325)
    else:

        m.fs.lean_fa_mixer.recycle_inlet.flow_mol[0].value = 10
        m.fs.lean_fa_mixer.recycle_inlet.mole_frac_comp[0, 'CH2O2'].value = 0.01
        m.fs.lean_fa_mixer.recycle_inlet.mole_frac_comp[0, 'H2O'].value = 0.99
        m.fs.lean_fa_mixer.recycle_inlet.temperature[0].value = 300
        m.fs.lean_fa_mixer.recycle_inlet.pressure[0].value = 101325

        m.fs.h2o_mixer.recycle_inlet.flow_mol[0].value = 10
        m.fs.h2o_mixer.recycle_inlet.mole_frac_comp[0, 'H2O'].value = 0.99
        m.fs.h2o_mixer.recycle_inlet.mole_frac_comp[0, 'O2'].value = 0.01
        m.fs.h2o_mixer.recycle_inlet.temperature[0].value = 300
        m.fs.h2o_mixer.recycle_inlet.pressure[0].value = 101325

    if dev_mode:

        m.fs.o2_separator_to_h2o_mixer = Arc(
            source=m.fs.o2_separator.water_outlet, destination=m.fs.h2o_mixer.recycle_inlet
        )
        m.fs.acid_separator_to_lean_fa_mixer = Arc(
            source=m.fs.acid_separator.water_outlet, destination=m.fs.lean_fa_mixer.recycle_inlet
        )
        TransformationFactory("network.expand_arcs").apply_to(m.fs)
        propagate_state(m.fs.acid_separator_to_lean_fa_mixer)
        m.fs.lean_fa_mixer.initialize(outlvl=idaeslog.INFO_LOW)
        propagate_state(m.fs.o2_separator_to_h2o_mixer)
        m.fs.h2o_mixer.initialize(outlvl=idaeslog.INFO_LOW)


    @m.fs.lean_fa_mixer.Constraint(m.fs.config.time, doc="Outlet pressure identical to makeup water inlet pressure")
    def lean_fa_mixer_pressure_eqn(b, t):
        return 1e-5*b.makeup_inlet.pressure[t] == 1e-5*b.outlet.pressure[t]
    @m.fs.h2o_mixer.Constraint(m.fs.config.time, doc="Outlet pressure identical to feed water outlet pressure")
    def h2o_mixer_pressure_eqn(b, t):
        return 1e-5*b.feed_inlet.pressure[t] == 1e-5*b.outlet.pressure[t]

    if dev_mode:

        dof = degrees_of_freedom(m)
        print(f"\nDegree of freedom = {dof}.\n")

        # solve the flowsheet
        solver = get_solver("ipopt")
        results = solver.solve(m, tee = False)

        m.fs.co2_mixer.recycle_inlet.flow_mol[:].unfix()
        @m.fs.Constraint(m.fs.config.time, doc="make co2 recycle flowrate same")
        def co2_recycle_eqn(b, t):
            return 1e-2*b.co2_mixer.recycle_inlet.flow_mol[t] == 1e-2*b.co2_separator.co2_outlet.flow_mol[t]

        dof = degrees_of_freedom(m)
        print(f"\nDegree of freedom = {dof}.\n")
        solver = get_solver("ipopt")
        results = solver.solve(m, tee = False)

        m.fs.co2_separator_to_co2_mixer = Arc(
            source=m.fs.co2_separator.co2_outlet, destination=m.fs.co2_mixer.recycle_inlet
        )

        TransformationFactory("network.expand_arcs").apply_to(m.fs)
        m.fs.co2_mixer.recycle_inlet.unfix()

        m.fs.co2_recycle_eqn.deactivate()
        dof = degrees_of_freedom(m)
        print(f"\nDegree of freedom = {dof}.\n")
        solver = get_solver("ipopt_v2")
        results = solver.solve(m, tee = False)



    # Constraint for the surrogate of ED model
    # Note that the split fraction of this expression could be > 1, which could cause convergence issue
    @m.fs.acid_separator.Constraint(m.fs.config.time, doc="Surrogate model for split factor")
    def surrogate_of_acid_separator_fa_in_water(b, t):
        X1 = b.inlet.mole_frac_comp[t,"CH2O2"]
        return (b.split_fraction[t, "water_outlet", "CH2O2"] ==
                    7936.78651463569 * X1
                    - 0.315153081391621 * log(X1)
                    - 7925.77121210169 * exp(X1)
                    + 3893.80434251668 * X1**2
                    + 1564.61726292076 * X1**3
                    + 7924.42563598759
        )

    @m.fs.acid_separator.Constraint(m.fs.config.time, doc="Surrogate model for split factor")
    def surrogate_of_acid_separator_h2o_in_prod(b, t):
        X1 = b.inlet.mole_frac_comp[t,"CH2O2"]
        return (b.split_fraction[t, "acid_outlet", "H2O"] ==
                    - 144.337497263043 * X1
                    + 5.867705325863298E-003 * log(X1)
                    + 144.313749616384 * exp(X1)
                    - 70.7482850184356 * X1**2
                    - 28.1812344617825 * X1**3
                    - 144.288852637820
        )

    if dev_mode:

        m.fs.acid_separator.split_fraction[0, "acid_outlet", 'CH2O2'].unfix()
        m.fs.acid_separator.split_fraction[0, "acid_outlet", 'H2O'].unfix()

        dof = degrees_of_freedom(m)
        print(f"\nDegree of freedom = {dof}.\n")
        solver = get_solver("ipopt")
        results = solver.solve(m, tee = False)


        m.fs.lean_fa_mixer_to_ez_module = Arc(
            source=m.fs.lean_fa_mixer.outlet, destination=m.fs.ez_module.acid_inlet
        )

        m.fs.h2o_mixer_to_ez_module = Arc(
            source=m.fs.h2o_mixer.outlet, destination=m.fs.ez_module.oxygen_inlet
        )

        TransformationFactory("network.expand_arcs").apply_to(m.fs)

        #propagate_state(m.fs.co2_mixer_to_ez_module)
        #propagate_state(m.fs.lean_fa_mixer_to_ez_module)
        #propagate_state(m.fs.h2o_mixer_to_ez_module)
        #m.fs.ez_module.initialize(outlvl=4)

        m.fs.ez_module.oxygen_inlet.unfix()
        m.fs.ez_module.acid_inlet.unfix()


    if not dev_mode:

        m.fs.membrane_to_compressor = Arc(
            source=m.fs.membrane.sweep_side_outlet, destination=m.fs.co2_feed_compressor.inlet
        )
        m.fs.compressor_to_co2_mixer = Arc(
            source=m.fs.co2_feed_compressor.outlet, destination=m.fs.co2_mixer.compressor_inlet
        )
        m.fs.co2_mixer_to_ez_module = Arc(
            source=m.fs.co2_mixer.outlet, destination=m.fs.ez_module.fuel_inlet
        )
        m.fs.ez_module_to_co2_separator = Arc(
            source=m.fs.ez_module.fuel_outlet, destination=m.fs.co2_separator.inlet
        )
        m.fs.co2_separator_to_co2_mixer = Arc(
            source=m.fs.co2_separator.co2_outlet, destination=m.fs.co2_mixer.recycle_inlet
        )
        m.fs.ez_module_to_acid_separator = Arc(
            source=m.fs.ez_module.acid_outlet, destination=m.fs.acid_separator.inlet
        )
        m.fs.acid_separator_to_lean_fa_mixer = Arc(
            source=m.fs.acid_separator.water_outlet, destination=m.fs.lean_fa_mixer.recycle_inlet
        )
        m.fs.lean_fa_mixer_to_ez_module = Arc(
            source=m.fs.lean_fa_mixer.outlet, destination=m.fs.ez_module.acid_inlet
        )
        m.fs.ez_module_to_o2_separator = Arc(
            source=m.fs.ez_module.oxygen_outlet, destination=m.fs.o2_separator.inlet
        )
        m.fs.o2_separator_to_h2o_mixer = Arc(
            source=m.fs.o2_separator.water_outlet, destination=m.fs.h2o_mixer.recycle_inlet
        )
        m.fs.h2o_mixer_to_ez_module = Arc(
            source=m.fs.h2o_mixer.outlet, destination=m.fs.ez_module.oxygen_inlet
        )
        TransformationFactory("network.expand_arcs").apply_to(m.fs)
        initializer = BlockTriangularizationInitializer(constraint_tolerance=2e-5)
        initializer.initialize(m.fs.membrane)
        propagate_state(m.fs.membrane_to_compressor)
        initializer.initialize(m.fs.co2_feed_compressor)
        propagate_state(m.fs.compressor_to_co2_mixer)
        m.fs.co2_mixer.initialize(outlvl=idaeslog.INFO_LOW)
        m.fs.lean_fa_mixer.initialize(outlvl=idaeslog.INFO_LOW)
        m.fs.h2o_mixer.initialize(outlvl=idaeslog.INFO_LOW)
        propagate_state(m.fs.co2_mixer_to_ez_module)
        propagate_state(m.fs.lean_fa_mixer_to_ez_module)
        propagate_state(m.fs.h2o_mixer_to_ez_module)
        m.fs.ez_module.initialize(outlvl=idaeslog.INFO_LOW)
        propagate_state(m.fs.ez_module_to_co2_separator)
        m.fs.co2_separator.initialize(outlvl=idaeslog.INFO_LOW)
        propagate_state(m.fs.ez_module_to_acid_separator)
        m.fs.acid_separator.initialize(outlvl=idaeslog.INFO_LOW)
        propagate_state(m.fs.ez_module_to_o2_separator)
        m.fs.o2_separator.initialize(outlvl=idaeslog.INFO_LOW)
        propagate_state(m.fs.co2_separator_to_co2_mixer)
        m.fs.co2_mixer.initialize(outlvl=idaeslog.INFO_LOW)
        propagate_state(m.fs.acid_separator_to_lean_fa_mixer)
        m.fs.lean_fa_mixer.initialize(outlvl=idaeslog.INFO_LOW)
        propagate_state(m.fs.o2_separator_to_h2o_mixer)
        m.fs.h2o_mixer.initialize(outlvl=idaeslog.INFO_LOW)
        m.fs.h2o_mixer.feed_inlet.flow_mol.unfix()
        m.fs.h2o_mixer.outlet.flow_mol.fix()
        m.fs.lean_fa_mixer.makeup_inlet.flow_mol.unfix()
        m.fs.lean_fa_mixer.outlet.flow_mol.fix()
        m.fs.co2_mixer.outlet.flow_mol.fix()
        m.fs.membrane.feed_side_inlet.flow_mol.unfix()
        dof = degrees_of_freedom(m)
        print(f"Degree of freedom = {dof}.")
        # solve the flowsheet
        solver = get_solver("ipopt")
        solver.solve(m, tee = False)



    # set the flowsheet and solve
    m.fs.h2o_mixer.feed_inlet.flow_mol.unfix()
    m.fs.h2o_mixer.outlet.flow_mol.fix()

    m.fs.lean_fa_mixer.makeup_inlet.flow_mol.unfix()
    m.fs.lean_fa_mixer.outlet.flow_mol.fix()

    #m.fs.co2_mixer.outlet.flow_mol.fix()
    #m.fs.membrane.feed_side_inlet.flow_mol.unfix()

    dof = degrees_of_freedom(m)
    print(f"\nDegree of freedom = {dof}.\n")

    # check model diagnostics
    dt = DiagnosticsToolbox(m, variable_bounds_violation_tolerance=1e-4)
    # TODO fix issues to remove "ignore" flags
    # dt.report_structural_issues()
    dt.assert_no_structural_warnings(ignore_evaluation_errors=True, ignore_unit_consistency=True)

    # solve the flowsheet
    solver = get_solver("ipopt_v2")
    results = solver.solve(m, tee = False)


    if dev_mode:

        print("=======================membrane results==========================")
        m.fs.membrane.feed_side_inlet.display()
        m.fs.membrane.sweep_side_outlet.display()
        print("=======================EZ results==========================")
        m.fs.ez_module.acid_inlet.display()
        m.fs.ez_module.oxygen_inlet.display()
        m.fs.ez_module.fuel_inlet.display()
        m.fs.ez_module.acid_outlet.display()
        m.fs.ez_module.oxygen_outlet.display()
        m.fs.ez_module.fuel_outlet.display()

        print("=======================TO CONNECT CO2 RECYCLE==========================")
        m.fs.co2_mixer.recycle_inlet.display()
        m.fs.co2_separator.co2_outlet.display()
        #m.fs.co2_mixer.compressor_inlet.display()
        #m.fs.co2_mixer.outlet.display()
        print("=======================TO CONNECT LEAN SOLUTION RECYCLE==========================")
        m.fs.lean_fa_mixer.outlet.display()
        m.fs.ez_module.acid_inlet.display()
        print("=======================TO CONNECT H2O RECYCLE==========================")
        m.fs.h2o_mixer.outlet.display()
        m.fs.ez_module.oxygen_inlet.display()

        print("=====================FINAL CHECK==========================")

        m.fs.acid_separator.acid_outlet.display()
        #42.777777778 mol/s is target
        m.fs.ez_module.electrochemical_cell.current_density.display()



    # co2_separator (PSA) model parameters and expressions
    m.fs.co2_separator.heat_cap = Param(initialize= 1.46,
        doc = "Average reversible heat capacity over the range 40–100 °C: 1.46 J/g•°C")

    m.fs.co2_separator.sensible_heat= Param (initialize = 88,
        doc = " Energy required to heat dmpn–Mg2(dobpdc) from 40°C to 100 °C: 88 J/g")

    m.fs.co2_separator.isosteric_heat = Param (initialize = 136,
        doc = "Average isosteric heat from 2.91 to 0.49 mmol/g is 136 kJ/kg")

    m.fs.co2_separator.total_heat = Param (initialize = 224,
        mutable=True,
        doc = " approximate energy required to heat dmpn–Mg2(dobpdc) is 224 kJ/kg ")

    m.fs.co2_separator.dobpdc_useage = Param(initialize = 9.36,
        doc = "Cycling 1 kg (22.72 mol) of CO2 requires 9.36 kg of dmpn–Mg2(dobpdc)")

    @m.fs.co2_separator.Expression(m.fs.config.time, doc="regeneration energy requirement")
    def regeneration_energy(b, t):
        return (b.total_heat * b.dobpdc_useage /1000
                * (b.co2_outlet.flow_mol[t]
                * b.co2_outlet.mole_frac_comp[t,'CO2']* 44.01/1000)
        )

    # Expressions for the surrogate of ED model
    @m.fs.acid_separator.Expression(m.fs.config.time, doc="Surrogate model for EDC condenser cooling duty")
    def edc_condenser_duty(b, t):
        X1 = b.inlet.mole_frac_comp[t,"CH2O2"]
        return b.mixed_state[t].flow_mass * (
                    25369756.0754989 * X1
                    - 13880.1454623206*log(X1)
                    - 32361960.3122260 * exp(X1)
                    + 24704647.1816597 * X1**2
                    + 35039019.3459239) * pyunits.J / pyunits.kg

    @m.fs.acid_separator.Expression(m.fs.config.time, doc="Surrogate model for EDC reboiler duty")
    def edc_reboiler_duty(b, t):
        X1 = b.inlet.mole_frac_comp[t,"CH2O2"]
        return b.mixed_state[t].flow_mass * (
                    -233315.546696040 * log(X1)
                    - 3933159.01227053 * exp(X1)
                    + 10683441.6033666 * X1**3
                    + 6429503.41954601) * pyunits.J / pyunits.kg

    @m.fs.acid_separator.Expression(m.fs.config.time, doc="Surrogate model for ERC condenser cooling duty")
    def erc_condenser_duty(b, t):
        X1 = b.inlet.mole_frac_comp[t,"CH2O2"]
        return b.mixed_state[t].flow_mass * (
                    -16135161938.9015 * X1
                    + 176098.429208634 * log(X1)
                    + 16126163370.3206 * exp(X1)
                    - 7958411891.85606 * X1**2
                    - 3135867025.29858 * X1**3
                    - 16125192208.5749) * pyunits.J / pyunits.kg

    @m.fs.acid_separator.Expression(m.fs.config.time, doc="Surrogate model for ERC reboiler duty")
    def erc_reboiler_duty(b, t):
        X1 = b.inlet.mole_frac_comp[t,"CH2O2"]
        return b.mixed_state[t].flow_mass * (
                    161265.706321033 * log(X1)
                    + 14348972.7097195 * X1**2
                    -35347054.7891300 * X1**3
                    + 692505.909373794) * pyunits.J / pyunits.kg

    @m.fs.acid_separator.Expression(m.fs.config.time, doc="Surrogate model for SULFO cooler duty")
    def sulfo_cooler_duty(b, t):
        X1 = b.inlet.mole_frac_comp[t,"CH2O2"]
        return b.mixed_state[t].flow_mass * (
                    -4361737879.24816 * X1
                    + 191802.517678019 * log(X1)
                    + 4355302093.28550 * exp(X1)
                    - 2138407278.15296 * X1**2
                    - 861639162.897247 * X1**3
                    - 4354058615.44534) * pyunits.J / pyunits.kg



    dof = degrees_of_freedom(m)
    print(f"\nDegree of freedom = {dof}.\n")


    iscale.calculate_scaling_factors(m)

    # solve the flowsheet
    solver = get_solver("ipopt_v2")
    results = solver.solve(m, tee = False)


    build_costing(
        m.fs,
        distillation_type="extractive",
        cost_year="2018_Dec",
        capacity_factor=0.85,
        )


    QGESSCostingData.costing_initialization(m.fs.costing)

    print("===============================Final Solve=================================")
    results = solver.solve(m, tee=True)


    report_costing_results(m.fs)

    return m


if __name__ == "__main__":
    #set up and run steady state model
    m = main_steady_state(optimize=False, dev_mode= True)
