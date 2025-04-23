#################################################################################
# The Institute for the Design of Advanced Energy Systems Integrated Platform
# Framework (IDAES IP) was produced under the DOE Institute for the
# Design of Advanced Energy Systems (IDAES).
#
# Copyright (c) 2018-2023 by the software owners: The Regents of the
# University of California, through Lawrence Berkeley National Laboratory,
# National Technology & Engineering Solutions of Sandia, LLC, Carnegie Mellon
# University, West Virginia University Research Corporation, et al.
# All rights reserved.  Please see the files COPYRIGHT.md and LICENSE.md
# for full copyright and license information.
#################################################################################
#
# TODO: Missing docstrings
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

# TODO: Look into protected access issues
# pylint: disable=protected-access

__author__ = "Douglas Allan, Jinliang Ma"

import copy

from pyomo.common.config import ConfigBlock, ConfigValue, In, Bool
import pyomo.environ as pyo


from idaes.core import declare_process_block_class, UnitModelBlockData
from idaes.core.util.constants import Constants
from . import common
from .common import (
    _gas_species_list,
    _all_species_list,
)
import idaes.core.util.scaling as iscale
from idaes.core.util.exceptions import ConfigurationError
from idaes.core.solvers import get_solver

import idaes.logger as idaeslog


@declare_process_block_class("SocTriplePhaseAcidBoundary")
class SocTriplePhaseAcidBoundaryData(UnitModelBlockData):
    CONFIG = ConfigBlock()
    CONFIG.declare(
        "dynamic",
        ConfigValue(
            domain=In([False]),
            default=False,
            description="Dynamic model flag",
            doc="No capacities or holdups, so no internal dynamics",
        ),
    )
    CONFIG.declare(
        "has_holdup",
        ConfigValue(domain=In([False]), default=False),
    )
    # This component list should include the components from both formic acid channel and the fuel channel
    # It can be assumed that all inert species enters the triple phase boundary but some of them will not
    # cross the boundary to the other side. e.g. N2 does not go the formic acid channel and formic acid does
    # not go to the porous fuel electrode pores.  The total pressure at the triple phase boundary is still fixed.
    # Note that innert species should all be included in this list to get the mole fractions correct
    # Note that the 'e^-' should not be in the list
    CONFIG.declare(
        "component_list",
        ConfigValue(
            domain=common._SubsetOf(_gas_species_list), description="List of components from both fuel and acid channels"
        ),
    )

    CONFIG.declare(
        "component_list_fuel",
        ConfigValue(
            domain=common._SubsetOf(_gas_species_list), description="List of components from fuel channel"
        ),
    )

    CONFIG.declare(
        "component_list_acid",
        ConfigValue(
            domain=common._SubsetOf(_gas_species_list), description="List of components from acid channel"
        ),
    )

    CONFIG.declare(
        "reaction_stoichiometry",
        ConfigValue(
            common._SubsetOf(_all_species_list),
            description="Stochiometric coefficients for component reactions on the triple phase boundary. Must contain "
            "term for number of electrons consumed/liberated.",
        ),
    )
    # Inert species from both fuel and formic acid channel
    CONFIG.declare(
        "inert_species",
        ConfigValue(
            default=None,
            domain=common._SubsetOf(_gas_species_list),
            description="List of species that do not participate in "
            "reactions at the triple phase boundary.",
        ),
    )
    # This is the original reference to the fuel channel concentration
    CONFIG.declare(
        "conc_mol_comp_ref_fuel",
        ConfigValue(
            default=None,
            description="Variable for the component concentration in bulk channel ",
        ),
    )
    # This is the extra reference to the formic acid channel concentration (assuming H2O + CH2O2)
    CONFIG.declare(
        "conc_mol_comp_ref_acid",
        ConfigValue(
            default=None,
            description="Variable for the component concentration in formic acid bulk channel ",
        ),
    )
    # hard-wired concentric deviation x for fuel and acid
    CONFIG.declare(
        "conc_mol_comp_deviation_x_fuel",
        ConfigValue(
            default=None,
            description="Variable for the component concentration in fuel electrode slab ",
        ),
    )
    CONFIG.declare(
        "conc_mol_comp_deviation_x_acid",
        ConfigValue(
            default=None,
            description="Variable for the component concentration in acid channel ",
        ),
    )
    CONFIG.declare(
        "material_flux_x_fuel",
        ConfigValue(
            default=None,
            description="Variable for the material flux to fuel side ",
        ),
    )
    CONFIG.declare(
        "material_flux_x_acid",
        ConfigValue(
            default=None,
            description="Variable for the material flux to the acid side ",
        ),
    )
    CONFIG.declare(
        "below_electrolyte",
        ConfigValue(
            domain=Bool,
            description="Whether the triple phase boundary is located below or "
            "above the electrolyte. This flag determines the sign of material_flux_x.",
        ),
    )
    CONFIG.declare(
        "voltage_drop_custom",
        ConfigValue(
            domain=Bool,
            default=False,
            description="If True, add voltage_drop_custom Var to be connected to degradation models",
        ),
    )

    common._submodel_boilerplate_config(CONFIG)
    # The following call declare temperature_z, heat_flux_x0, and heat flux_x1
    common._thermal_boundary_conditions_config(CONFIG, thin=True)
    #common._material_boundary_conditions_config(CONFIG, thin=True)
    #material boundary condition config is hard-wired

    def build(self):
        super().build()

        # Set up some sets for the space and time indexing
        tset = self.flowsheet().config.time
        # Set up node and face sets and get integer indices for them
        izfaces, iznodes = common._face_initializer(  # pylint: disable=unused-variable
            self, self.config.control_volume_zfaces, "z"
        )

        comps = self.component_list = pyo.Set(
            initialize=self.config.component_list,
            ordered=True,
            doc="Set of all gas-phase components present in submodel",
        )
        comps.display()
        
        comps_fuel = self.component_list_fuel = pyo.Set(
            initialize=self.config.component_list_fuel,
            ordered=True,
            doc="Set of all gas-phase components present in fuel channel and fuel electrode",
        )

        comps_acid = self.component_list_acid = pyo.Set(
            initialize=self.config.component_list_acid,
            ordered=True,
            doc="Set of all gas-phase components present in acid channel",
        )

        if "e^-" not in self.config.reaction_stoichiometry.keys():
            raise ConfigurationError(
                f"Number of electrons produced or consumed in redox reaction at {self.name} "
                "not specified."
            )

        self.reaction_stoichiometry = copy.copy(self.config.reaction_stoichiometry)

        if self.config.inert_species is None:
            self.config.inert_species = list()
        # Copy and pasted from the Gibbs reactor
        for j in self.config.inert_species:
            if j not in comps:
                raise ConfigurationError(
                    "{} invalid component in inert_species argument. {} is "
                    "not in the provided component list.".format(self.name, j)
                )

        self.inert_species_list = pyo.Set(
            initialize=self.config.inert_species,
            ordered=True,
            doc="Set of components that do not react at triple phase boundary",
        )
        # Ensure all inerts have been assigned a zero for a stoichiometric coefficient
        for j in self.inert_species_list:
            try:
                # Want to future-proof this method in case floating-point round-off ever becomes an issue.
                if abs(self.reaction_stoichiometry[j]) > 1e-8:
                    raise ConfigurationError(
                        f"Component {j} was in inert_species_list provided to {self.name}, but "
                        "has a nonzero stoichiometric coefficient."
                    )
            except KeyError:
                # Inert species does not have stoichiometry specified.
                pass
            self.reaction_stoichiometry[j] = 0

        # JM. it seems that the reacting component list contains electron while reacting gas list does not
        self.reacting_component_list = pyo.Set(
            initialize=[
                j
                for j, coeff in self.reaction_stoichiometry.items()
                if j not in self.inert_species_list
            ],
            ordered=True,
            doc="Set of components (gas-phase and solid) that react at triple "
            "phase boundary",
        )
        self.reacting_gas_list = pyo.Set(
            initialize=[j for j in comps if j not in self.inert_species_list],
            ordered=True,
            doc="Set of gas-phase components that react at triple phase boundary",
        )
        
        self.reacting_gas_list.display()

        common._submodel_boilerplate_create_if_none(self)
        
        # this will create temperature_deviation_x and expression for temperature
        common._create_thermal_boundary_conditions_if_none(self, thin=True)
        
        #common._create_material_boundary_conditions_if_none(self, thin=True)

        common._create_if_none(
            self,
            "conc_mol_comp_deviation_x_fuel",
            idx_set=(tset, iznodes, comps_fuel),
            units=pyo.units.mol / pyo.units.m**3,
        )

        common._create_if_none(
            self,
            "conc_mol_comp_deviation_x_acid",
            idx_set=(tset, iznodes, comps_acid),
            units=pyo.units.mol / pyo.units.m**3,
        )


        # create flux for fuel and acid channel fluxes separately so they can be used for individual boundary conditions
        common._create_if_none(
            self,
            "material_flux_x_fuel",
            idx_set=(tset, iznodes, comps_fuel),
            units=pyo.units.mol / (pyo.units.s * pyo.units.m**2),
        )

        common._create_if_none(
            self,
            "material_flux_x_acid",
            idx_set=(tset, iznodes, comps_acid),
            units=pyo.units.mol / (pyo.units.s * pyo.units.m**2),
        )
        
        common._create_if_none(
            self,
            "conc_mol_comp_ref_fuel",
            idx_set=(tset, iznodes, comps_fuel),
            units=pyo.units.mol / pyo.units.m**3,
        )

        common._create_if_none(
            self,
            "conc_mol_comp_ref_acid",
            idx_set=(tset, iznodes, comps_acid),
            units=pyo.units.mol / pyo.units.m**3,
        )

        # since temperature is hard to solve for the triple phase boundary, we have to fix the operating temperature
        self.operating_temperature = pyo.Var(
            initialize=298.15,
            units=pyo.units.K,
        )
        self.operating_temperature.fix()

        # to be calculated by a Constraint
        self.mole_frac_comp = pyo.Var(
            tset,
            iznodes,
            comps,
            initialize=1 / len(comps),
            units=pyo.units.dimensionless,
            bounds=(0, 1),
        )

        self.log_mole_frac_comp = pyo.Var(
            tset,
            iznodes,
            self.reacting_gas_list,
            initialize=-1,
            units=pyo.units.dimensionless,
            bounds=(None, 0),
        )

        self.activation_potential = pyo.Var(
            tset,
            iznodes,
            initialize=-1,
            units=pyo.units.V,
            bounds=(-6, 0),
        )

        self.activation_potential_alpha1 = pyo.Var(
            initialize=0.5,
            units=pyo.units.dimensionless,
        )

        self.activation_potential_alpha2 = pyo.Var(
            initialize=0.5,
            units=pyo.units.dimensionless,
        )

        self.exchange_current_exponent_comp = pyo.Var(
            self.reacting_gas_list,
            initialize=1,
            units=pyo.units.dimensionless,
            bounds=(0, None),
        )

        # Nominally dimensionless, but specifically the log of the preexponential factor in amps per m**2
        self.exchange_current_log_preexponential_factor = pyo.Var(
            initialize=1, units=pyo.units.dimensionless, bounds=(0, None)
        )

        self.exchange_current_activation_energy = pyo.Var(
            initialize=0, units=pyo.units.J / pyo.units.mol, bounds=(0, None)
        )
        if self.config.voltage_drop_custom:
            self.voltage_drop_custom = pyo.Var(
                tset,
                iznodes,
                units=pyo.units.volts,
                doc="Custom voltage drop term for degradation modeling",
            )

        # We need to get concentrations from acid channel side for formic acid, H2O and from electrode side for CO2
       
        @self.Expression(tset, iznodes, comps)
        def conc_mol_comp(b, t, iz, j):
            if j in b.component_list_fuel:
                return b.conc_mol_comp_ref_fuel[t, iz, j] + b.conc_mol_comp_deviation_x_fuel[t, iz, j]
            elif j in b.component_list_acid:
                return b.conc_mol_comp_ref_acid[t, iz, j] + b.conc_mol_comp_deviation_x_acid[t, iz, j]

        # actually pressure is fixed and concentration or its deviation is calculated based on pressure
        @self.Expression(tset, iznodes)
        def pressure(b, t, iz):
            return (
                sum(b.conc_mol_comp[t, iz, i] for i in comps)
                * Constants.gas_constant
                * b.temperature[t, iz]
            )

        # mole_frac_comp must be a variable because we want IPOPT to enforce
        # a lower bound of 0 in order to avoid AMPL errors, etc.
        @self.Constraint(tset, iznodes, comps)
        def mole_frac_comp_eqn(b, t, iz, j):
            return b.mole_frac_comp[t, iz, j] == b.conc_mol_comp[t, iz, j] / sum(
                b.conc_mol_comp[t, iz, i] for i in comps
            )

        @self.Constraint(tset, iznodes, self.reacting_gas_list)
        def log_mole_frac_comp_eqn(b, t, iz, j):
            return b.mole_frac_comp[t, iz, j] == pyo.exp(b.log_mole_frac_comp[t, iz, j])

        @self.Expression(tset, iznodes)
        def ds_rxn(b, t, iz):
            T = b.temperature[t, iz]
            P = b.pressure[t, iz]
            P_ref = 1e5 * pyo.units.Pa
            log_y_j = b.log_mole_frac_comp
            nu_j = b.reaction_stoichiometry
            # Any j not in comps is assumed to not be vapor phase
            pressure_exponent = sum(nu_j[j] for j in b.reacting_gas_list)
            if abs(pressure_exponent) < 1e-6:
                out_expr = 0
            else:
                out_expr = (
                    -Constants.gas_constant * pressure_exponent * pyo.log(P / P_ref)
                )
            return out_expr + (
                    sum(
                    nu_j[j] * common._comp_entropy_expr(T, j)
                    for j in b.reacting_component_list
                )
                - Constants.gas_constant
                * sum(
                    nu_j[j] * log_y_j[t, iz, j]
                    for j in b.reacting_gas_list
                    # TODO verify that excluding solids is correct
                )
            )

        @self.Expression(tset, iznodes)
        def dh_rxn(b, t, iz):
            return sum(
                b.reaction_stoichiometry[j]
                * common._comp_enthalpy_expr(b.temperature[t, iz], j)
                for j in b.reacting_component_list
            )

        @self.Expression(tset, iznodes)
        def dg_rxn(b, t, iz):
            return b.dh_rxn[t, iz] - b.temperature[t, iz] * b.ds_rxn[t, iz]

        @self.Expression(tset, iznodes)
        def potential_nernst(b, t, iz):
            if b.config.below_electrolyte:
                return -b.dg_rxn[t, iz] / (
                    Constants.faraday_constant * b.reaction_stoichiometry["e^-"]
                )
            else:
                return -b.dg_rxn[t, iz] / (
                    Constants.faraday_constant * -b.reaction_stoichiometry["e^-"]
                )

        @self.Expression(tset, iznodes)
        def log_exchange_current_density(b, t, iz):
            T = b.operating_temperature #b.temperature[t, iz]
            log_k = b.exchange_current_log_preexponential_factor[None]
            expo = b.exchange_current_exponent_comp
            E_A = b.exchange_current_activation_energy[None]
            out = log_k - E_A / (Constants.gas_constant * T)
            for j in b.reacting_gas_list:
                out += expo[j] * b.log_mole_frac_comp[t, iz, j]
            return out

        # Butler Volmer equation
        @self.Constraint(tset, iznodes)
        def activation_potential_eqn(b, t, iz):
            i = b.current_density[t, iz]
            log_i0 = b.log_exchange_current_density[t, iz]
            eta = b.activation_potential[t, iz]
            T = b.operating_temperature #b.temperature[t, iz]
            alpha_1 = b.activation_potential_alpha1[None]
            alpha_2 = b.activation_potential_alpha2[None]
            exp_expr = Constants.faraday_constant * eta / (Constants.gas_constant * T)
            return i == pyo.units.A / pyo.units.m**2 * (
                pyo.exp(log_i0 + alpha_1 * exp_expr)
                - pyo.exp(log_i0 - alpha_2 * exp_expr)
            )

        @self.Expression(tset, iznodes)
        def reaction_rate_per_unit_area(b, t, iz):
            # Assuming there are no current leaks, the reaction rate can be
            # calculated directly from the current density
            if b.config.below_electrolyte:
                return b.current_density[t, iz] / (
                    Constants.faraday_constant * b.reaction_stoichiometry["e^-"]
                )
            else:
                return b.current_density[t, iz] / (
                    Constants.faraday_constant * -b.reaction_stoichiometry["e^-"]
                )

        # Put this expression in to prepare for a contact resistance term
        @self.Expression(tset, iznodes)
        def voltage_drop_total(b, t, iz):
            if self.config.voltage_drop_custom:
                return b.activation_potential[t, iz] + b.voltage_drop_custom[t, iz]
            else:
                return b.activation_potential[t, iz]

        @self.Constraint(tset, iznodes)
        def heat_flux_x_eqn(b, t, iz):
            return (
                b.heat_flux_x1[t, iz]
                == b.heat_flux_x0[t, iz]
                + b.current_density[t, iz]
                * b.voltage_drop_total[t, iz]  # Resistive heating
                - b.reaction_rate_per_unit_area[t, iz]  # Reversible heat of reaction
                * b.temperature[t, iz]
                * b.ds_rxn[t, iz]
            )

        @self.Constraint(tset, iznodes, comps)
        def material_flux_x_eqn(b, t, iz, j):
            if j in b.component_list_fuel:  # CO2 as product to flow down in fuel-cell mode, actually the rate is negative
                return (
                    b.material_flux_x_fuel[t, iz, j]
                    == -b.reaction_rate_per_unit_area[t, iz]
                    * b.reaction_stoichiometry[j]
                )
            else:  # the reactant formic acid actually flow up since rate is negative and stoichiometric coeff is negative
                return (
                    b.material_flux_x_acid[t, iz, j]
                    == b.reaction_rate_per_unit_area[t, iz]
                    * b.reaction_stoichiometry[j]
                )

    def initialize_build(
        self, outlvl=idaeslog.NOTSET, solver=None, optarg=None, fix_x0=False
    ):
        solve_log = idaeslog.getSolveLogger(self.name, outlvl, tag="unit")

        self.temperature_deviation_x.fix()
        self.conc_mol_comp_ref_fuel.fix()
        self.conc_mol_comp_ref_acid.fix()
        self.conc_mol_comp_deviation_x_fuel.fix()
        self.conc_mol_comp_deviation_x_acid.fix()
        if fix_x0:
            self.heat_flux_x0.fix()
        else:
            self.heat_flux_x1.fix()

        for t in self.flowsheet().time:
            for iz in self.iznodes:
                denom = pyo.value(
                    sum(self.conc_mol_comp[t, iz, j] for j in self.component_list)
                )
                for j in self.component_list:
                    self.mole_frac_comp[t, iz, j].value = pyo.value(
                        self.conc_mol_comp[t, iz, j] / denom
                    )
                    if j in self.reacting_gas_list:
                        self.log_mole_frac_comp[t, iz, j].value = pyo.value(
                            pyo.log(self.mole_frac_comp[t, iz, j])
                        )

        solver_obj = get_solver(solver, optarg)
        common._init_solve_block(self, solver_obj, solve_log)

        self.temperature_deviation_x.unfix()
        self.conc_mol_comp_ref_fuel.unfix()
        self.conc_mol_comp_ref_acid.unfix()
        self.conc_mol_comp_deviation_x_fuel.unfix()
        self.conc_mol_comp_deviation_x_acid.unfix()
        if fix_x0:
            self.heat_flux_x0.unfix()
        else:
            self.heat_flux_x1.unfix()

    def calculate_scaling_factors(self):
        pass

    def recursive_scaling(self):
        gsf = iscale.get_scaling_factor

        def ssf(c, s):
            iscale.set_scaling_factor(c, s, overwrite=False)

        sgsf = iscale.set_and_get_scaling_factor

        def cst(c, s):
            iscale.constraint_scaling_transform(c, s, overwrite=False)

        sy_def = 10  # Mole frac comp scaling

        for t in self.flowsheet().time:
            for iz in self.iznodes:
                ssf(self.activation_potential[t, iz], 10)
                if self.current_density[t, iz].is_reference():
                    si = gsf(self.current_density[t, iz].referent, default=1e-2)
                else:
                    si = gsf(self.current_density[t, iz], default=1e-2, warning=True)
                # TODO come back when I come up with a good formulation
                cst(self.activation_potential_eqn[t, iz], si)
                if self.heat_flux_x0[t, iz].is_reference():
                    gsf(self.heat_flux_x0[t, iz].referent, default=1e-2)
                else:
                    sqx0 = sgsf(self.heat_flux_x0[t, iz], 1e-2)
                if self.heat_flux_x1[t, iz].is_reference():
                    sqx1 = gsf(self.heat_flux_x1[t, iz].referent, 1e-2)
                else:
                    sqx1 = sgsf(self.heat_flux_x1[t, iz], 1e-2)
                sqx = min(sqx0, sqx1)
                cst(self.heat_flux_x_eqn[t, iz], sqx)

                for j in self.component_list:
                    # TODO Come back with good formulation for trace components
                    # and scale DConc and Cref
                    sy = sgsf(self.mole_frac_comp[t, iz, j], sy_def)
                    cst(self.mole_frac_comp_eqn[t, iz, j], sy)
                    if j in self.component_list_fuel:
                        smaterial_flux_x = sgsf(self.material_flux_x_fuel[t, iz, j], 1e-2)
                    else:
                        smaterial_flux_x = sgsf(self.material_flux_x_acid[t, iz, j], 1e-2)
                    cst(self.material_flux_x_eqn[t, iz, j], smaterial_flux_x)
                    if j in self.reacting_gas_list:
                        ssf(self.log_mole_frac_comp[t, iz, j], 1)
                        cst(self.log_mole_frac_comp_eqn[t, iz, j], sy)
