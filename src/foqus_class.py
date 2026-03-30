#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# foqus_class.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import os
import logging
from pathlib import Path
import json
import pandas as pd

from netlolca.NetlOlca import NetlOlca
import prommis.uky.uky_flowsheet as uky
from pyomo.core.base.var import Var
import foqus_lib.framework.graph.graph as gr
import foqus_lib.framework.graph.nodeVars as nv
from foqus_lib.framework.uq.Distribution import Distribution
from foqus_lib.framework.session.session import session
from foqus_lib.framework.optimizer.problem import objectiveFunction, inequalityConstraint
import foqus_lib.framework.optimizer.NLopt as nlopt
from pyomo.environ import value

import src as lca_prommis

##############################################################################
# CLASSES
##############################################################################
class NetlFoqus(object):
    # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    # Class Initialization
    # ////////////////////////////////////////////////////////////////////////
    def __init__(self):
        """Initialize NetlFoqus class."""
        self.vars = []  # all variables
        self.dv = []    # user-defined decision variables
        self.exchanges = pd.DataFrame()  # user-defined exchanges
        self.exchanges_vars = []  # variables of user-defined exchanges
        self.fs = None  # foqus flowsheet object
        self.prommis_node = None  # ProMMiS node object
        self.olca_node = None     # openLCA node object
        self.edge = None    # edge object
        self.logger = logging.getLogger(__name__)
        self.outVars = {} # output variables with corresponding nodes
        self.output_dir = lca_prommis.setup_output_directory(
            lca_prommis.output_dir
            )

    # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    # Class Property Definitions
    # ////////////////////////////////////////////////////////////////////////
    @property
    def dv_names(self):
        # Decision variable names
        return [x.ipvname for x in self.dv]

    @property
    def ndv(self):
        # Non-decision variables
        return [x for x in self.vars if x not in self.dv]

    @ndv.setter
    def ndv(self, value):
        raise AttributeError("ndv is a read-only property")

    @property
    def has_graph(self):
        return self.fs is not None
        # [FH] changed this to has_graph
        # the graph lives within a session
        # create_session function changed to create_graph
        # new create_session function added

    @has_graph.setter
    def has_graph(self, value):
        raise AttributeError("has_graph is a read-only property")

    # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    # Class Function Definitions
    # ////////////////////////////////////////////////////////////////////////
    def add_decision_variable(self, var_name):
        """Add a decision variable to the FOQUS flowsheet.

        Parameters
        ----------
        var_name : str
            The name of the decision variable to be added.

        Returns
        -------
        None
        """
        if var_name not in self.vars:
            raise ValueError(
                f"Variable {var_name} not found in flowsheet variables."
            )
        if var_name in self.dv_names:
            self.logger.warning(
                f"Variable {var_name} is already a decision variable."
            )
        else:
            self.dv.append(nv.NodeVars(ipvname=var_name))


    def add_edge(self, from_node, to_node):
        """Add an edge to the FOQUS flowsheet.

        Parameters
        ----------
        from_node : nd.Node
            The starting node of the edge.
        to_node : nd.Node
            The ending node of the edge.

        Returns
        -------
        ed.Edge
            The created edge object.
        """
        if not self.has_graph:
            raise RuntimeError(
                "Flowsheet graph not created. Call create_graph() first."
            )

        # check if the from_node is a valid object
        if not isinstance(from_node, gr.Node):
            raise TypeError("from_node must be a valid FOQUS node object")
        # check if the to_node is a valid object
        if not isinstance(to_node, gr.Node):
            raise TypeError("to_node must be a valid FOQUS node object")

        # Add edge to edges list; uses node names
        self.fs.addEdge(from_node.name, to_node.name)

        # Find and return the created edge
        return self.fs.edges[-1]


    def add_node(self, node_name):
        """Add a node to the FOQUS flowsheet.

        Parameters
        ----------
        node_name : str
            The name of the node to be added.

        Returns
        -------
        nd.Node
            The created node object.
        """
        if not self.has_graph:
            raise RuntimeError(
                "Flowsheet session not created. Call create_graph() first."
            )

        node = self.fs.addNode(node_name)
        return node

    def create_graph(self, graph_name):
        """Create a FOQUS flowsheet graph.

        Parameters
        ----------
        session_name : str
            The name of the FOQUS flowsheet session.

        Returns
        -------
        None
        """
        self.fs = gr.Graph(graph_name)

    def set_dv_max(self, var_name, max_val):
        """Set the maximum value for a decision variable.

        Parameters
        ----------
        var_name : str
            The name of the decision variable.
        max_val : float
            The maximum value to set.

        Returns
        -------
        None
        """
        for dv in self.dv:
            if dv.ipvname == var_name:
                dv.setMax(max_val)
                return
        raise ValueError(f"Decision variable {var_name} not found.")

    def set_dv_min(self, var_name, min_val):
        """Set the minimum value for a decision variable.

        Parameters
        ----------
        var_name : str
            The name of the decision variable.
        min_val : float
            The minimum value to set.

        Returns
        -------
        None
        """
        for dv in self.dv:
            if dv.ipvname == var_name:
                dv.setMin(min_val)
                return
        raise ValueError(f"Decision variable {var_name} not found.")

    def set_dv_value(self, var_name, value):
        """Set the value for a decision variable.

        Parameters
        ----------
        var_name : str
            The name of the decision variable.
        value : float
            The value to set.

        Returns
        -------
        None
        """
        for dv in self.dv:
            if dv.ipvname == var_name:
                dv.setValue(value)
                return
        raise ValueError(f"Decision variable {var_name} not found.")

    def set_dv_dist(self, var_name, distribution):
        """Set the distribution for a decision variable.

        Parameters
        ----------
        var_name : str
            The name of the decision variable.
        distribution : distribution object
            The distribution to set.

        Returns
        -------
        None

        Throws
        ------
        ValueError
            If the decision variable type is not found.
            See Distribution.fullNames or Distribution.psuadeNames
        """
        # Distribution is title case (e.g., "Uniform" or "U")
        if isinstance(distribution, str):
            distribution = distribution.title()
        my_dist = Distribution(distribution)

        for dv in self.dv:
            if dv.ipvname == var_name:
                dv.dist = my_dist
                return
        raise ValueError(f"Decision variable {var_name} not found.")

    def init_uky(self):
        """
        Initialize the UKy flowsheet.

        Throws
        ------
        TypeError
            If the producing_node or receiving_node is not a valid FOQUS node object
        ValueError
            If the exchanges table is not found


        Returns
        -------
        the UKy flosheet model
        
        """
        # Creates a session with ProMMiS and openLCA nodes connected by an edge.
        # Reads the UKy exchange table and populates the variable list.

        # Create FOQUS flowsheet
        self.create_graph("UKy REE Flowsheet")
        self.prommis_node = self.add_node("ProMMiS")
        self.olca_node = self.add_node("openLCA")
        self.edge = self.add_edge(self.prommis_node, self.olca_node)

        # Get UKy exchange table
        my_vars, my_df, my_cm = get_uky_vars_exchanges()

        self.vars = my_vars

        # Add exchange variables names from exchange flow names:
        self.exchanges = my_df

        return my_cm
    
    def set_input_variables(self, node, var_name, var_value, var_min, var_max):
        """
        Set the input variables for a given node.
        
        Parameters
        ----------
        node : gr.Node
            The node to set the output variables for.
        var_name : str
            The name of the input variable.
        var_value : float
            The value of the input variable.
        var_min : float
            The minimum value of the input variable.
        var_max : float
            The maximum value of the input variable.

        Returns
        -------
        The input variable object
        """
        if not isinstance(node, gr.Node):
            raise TypeError("node must be a valid FOQUS node object")
        var = nv.NodeVars(ipvname=var_name, dtype = float)
        if var.ipvname not in self.vars:
            self.vars.append(var.ipvname)
        var.setValue(var_value)
        var.setMin(var_min)
        var.setMax(var_max)
        var.scaling = "Linear"
        if node is not None:
            node.inVars[var_name] = var
        logging.info(
            "Set input variable %s for node %s: value=%f" % (var_name, node.name, var_value)
        )
        return self.vars[-1]

    def initialize_intermediate_variables(self, producing_node, receiving_node):
        """
        Initialize the intermediate variables.

        Parameters
        ----------
        producing_node : gr.Node
            The producing node producing an output.
        receiving_node : gr.Node
            The receiving node receiving the outputs of the producing node.

        Returns
        -------
        None
        """
        exchanges_names = self.exchanges['Flow_Name'].tolist()
        if not isinstance(producing_node, gr.Node):
            raise TypeError("producing_node must be a valid FOQUS node object")
        if not isinstance(receiving_node, gr.Node):
            raise TypeError("receiving_node must be a valid FOQUS node object")
        for var_name in exchanges_names:
            self.vars.append(nv.NodeVars(opvname=var_name, dtype = float).opvname)
            self.exchanges_vars.append(nv.NodeVars(opvname=var_name, dtype = float))
            value = self.exchanges.loc[self.exchanges['Flow_Name'] == var_name, 'LCA_Amount'].values[0]
            self.exchanges_vars[-1].setValue(value)
            logging.info(
                "Set output properties for %s: value=%f" % (var_name, value)
            )
            if producing_node is not None:
                producing_node.outVars[var_name] = self.exchanges_vars[-1]
            if producing_node.name not in self.outVars:
                self.outVars[producing_node.name] = []
            self.outVars[producing_node.name].append(var_name)

        for var_name in exchanges_names:
            input_var_name = var_name + "_input"
            self.vars.append(nv.NodeVars(ipvname=input_var_name, dtype = float).ipvname)
            self.exchanges_vars.append(nv.NodeVars(ipvname=input_var_name, dtype = float))
            value = self.exchanges.loc[self.exchanges['Flow_Name'] == var_name, 'LCA_Amount'].values[0]
            self.exchanges_vars[-1].setValue(value)
            logging.info(
                "Set input properties for %s: value=%f" % (input_var_name, value)
            )
            if receiving_node is not None:
                receiving_node.inVars[input_var_name] = self.exchanges_vars[-1]

    def connect_intermediate_variables(self, node1, node2):
        """
        Connect the intermediate variables.
        
        Parameters
        ----------
        node1 : gr.Node
            The producing node.
        node2 : gr.Node
            The receiving node.

        Returns
        -------
        None

        """
        for var_out in node1.outVars:
            var_in = next(var for var in node2.inVars if var.startswith(var_out))
            self.edge.addConnection(var_out, var_in)
            logging.info(
                "Connected %s to %s" % (var_out, var_in)
            )
    
    def initiate_output_variables(self, node, var_name, var_value):
        """
        Initiate an output variable for a given node.
        
        Parameters
        ----------
        node : gr.Node
            The node to initiate the output variable for.
        var_name : str
            The name of the output variable.
        var_value : float
            The value of the output variable.

        Returns
        -------
        None
        """
        if not isinstance(node, gr.Node):
            raise TypeError("node must be a valid FOQUS node object")
        var = nv.NodeVars(opvname=var_name, dtype = float)
        if var.opvname not in self.vars:
            self.vars.append(var.opvname)
        var.setValue(var_value)
        if node is not None:
            node.outVars[var_name] = var
        if node.name not in self.outVars:
            self.outVars[node.name] = []
        self.outVars[node.name].append(var.opvname)
        logging.info(
            "Initiated output variable %s for node %s: value=%f" % (var_name, node.name, var_value)
        )
        return self.vars[-1]

    def define_node_script(self, node, script):
        """
        This function defines the script for a give node

        Parameters
        ----------
        node : gr.Node
            The node to define the script for.
        script : str
            The script to define for the node.

        Returns
        -------
        None
        """
        if not isinstance(node, gr.Node):
            raise TypeError("node must be a valid FOQUS node object")
        
        node.pythonCode = script
        logging.info(
            "Defined script for node %s: script=%s" % (node.name, script)
        )

    def set_node_scriptMode(self, node, script_mode):
        """
        Set the script mode for a given node.
        
        Parameters
        ----------
        node : gr.Node
            The node to set the script mode for.
        script_mode : str
            The script mode to set.
            Options include:
            'pre' --> runs the script before the model
            'total' --> runs the script instead of the model
            'post' --> runs the script after the model

        Returns
        -------
        None
        """
        if not isinstance(node, gr.Node):
            raise TypeError("node must be a valid FOQUS node object")
        
        if script_mode not in ['pre', 'total', 'post']:
            raise ValueError("script_mode must be either 'pre', 'total', or 'post'")
        node.scriptMode = script_mode
        logging.info(
            "Set script mode for node %s: script_mode=%s" % (node.name, script_mode)
        )
        
    def run_standalone_node_script(self, node):
        """
        Run the script for a given node.
        This will run the script as if the node 
        scriptMode is 'total'.
        
        Parameters
        ----------
        node : gr.Node
            The node to run the script for.
        """
        if not isinstance(node, gr.Node):
            raise TypeError("node must be a valid FOQUS node object")
        if node.pythonCode is None:
            raise ValueError("node has no script to run")
        
        node.runPython()
        logging.info(
            "Run script for node %s" % (node.name)
        )
        
    def run_node_script (self, node):
        """
        Run the script for a given node.
        This function will run the script based on 
        the set node scriptMode.

        Options include:
        'pre' --> runs the script before the model
        'total' --> runs the script instead of the model
        'post' --> runs the script after the model
        
        Parameters
        ----------
        node : gr.Node
            The node to run the script for.
        """
        if not isinstance(node, gr.Node):
            raise TypeError("node must be a valid FOQUS node object")
        if node.pythonCode is None:
            raise ValueError("node has no script to run")
        
        node.runCalc()
        logging.info(
            "Run script for node %s with script mode %s" % (node.name, node.scriptMode)
        )

    def create_session(self):
        """
        Creates a session to run the optimization.

        Returns
        -------
        session : foqus_lib.framework.session.session
            The session object.
        """

        cwd = os.getcwd()
        foqus_wd = get_foqus_wd()
        if cwd != foqus_wd:
            os.chdir(foqus_wd)
            logging.info(
                "Changed working directory to %s" % foqus_wd
            )
        else:
            logging.info(
                "Working directory is already %s" % cwd
            )
         
        my_session = session(useCurrentWorkingDir=True)

        # FOQUS optimizers expect flowsheet to be the Graph (gr.Graph) with .input, not NetlFoqus
        my_session.flowsheet = self.fs
        # Graph.copyGraph() and runListAsThread expect these session attributes on the flowsheet
        self.fs.pymodels = my_session.pymodels
        self.fs.pymodels_ml_ai = my_session.pymodels_ml_ai
        self.fs.resubMax = getattr(my_session, "resubMax", 0)

        return my_session

    def validate_node_script(self, node):
        """
        Run a node script in standalone mode and validate it completes without errors.
        
        Parameters
        ----------
        node : gr.Node
            The node to run the script for.
        
        Returns
        -------
        bool
            True if script runs without errors, False otherwise.
        """
        try:
            # Run the node script
            self.run_standalone_node_script(node)
            
            # Check for calculation errors on the node
            if node.calcError != 0:
                logging.error(
                    f"Node script execution failed for {node.name}. "
                    f"Error code: {node.calcError}"
                )
                return False
            
            logging.info(f"Node script executed successfully for {node.name}")
            return True
            
        except Exception as e:
            logging.error(
                f"Exception occurred while running node script for {node.name}: {str(e)}"
            )
            return False
    
    def setup_optimizer(self, session, solver_name, source_node):
        """
        This function setups the optimizer for the session

        It creates a problem object, assisngs the solver name, 
        and appends the decision variables to the problem
        
        Parameters
        ----------
        session : foqus_lib.framework.session.session
            The session object.
        solver_name : str
            The name of the solver.
        source_node : nv.NodeVars
            The node that has the inputs to be set as decision variables

        Returns
        -------
        problem : foqus_lib.framework.optimizer.problem
            The problem object.
        """
        # TODO: Add error handling for solver name
        #       solver name should be checked for a list
        #       this class currently only supports NLopt
        problem = session.optProblem
        problem.solver = solver_name
        # HOTFIX: append decision variables rather than assign 
        # them which results in overwriting an old dv with the following one 
        for dv in self.dv:
            problem.v.append(f"{source_node.name}.{dv.ipvname}")
        
        return problem

    def create_problem_objective_singular (self, problem, objectives_list, output_nodes_list, failure_val_list, penalty_scale_list):
        """
        This function creates a problem objective
        
        Parameters
        ----------
        problem : foqus_lib.framework.optimizer.problem
            The problem object.
        objectives_list : list
            The list of objective names.
            example: ['GWP', 'CED']
        output_nodes_list : list
            The list of output nodes.
            example: [nf.olca_node, nf.prommis_node]
            Name of the node whose outputs are used as objectives (e.g. "openLCA" for GWP/CED).
        failure_val_list : list
            The failure value for each objective.
            example: [10, 10]
            Note: the failure value should be higher than the expected highest value for a successful objective.
        penalty_scale_list : list
            The penalty scale for each objective.

        Returns
        -------
        problem : foqus_lib.framework.optimizer.problem
            The problem object.
        """
        problem.obj = []
        for idx, obj in enumerate(objectives_list):

            if not (len(objectives_list) == len(output_nodes_list) == len(failure_val_list) == len(penalty_scale_list)):
                raise IndexError("Lists must have the same number of elements!")

            objective_function = objectiveFunction()
            objective_function.pycode = f'f["{output_nodes_list[idx].name}"]["{obj}"]'
            if len(penalty_scale_list) == 1:
                objective_function.penScale = 1
            else:
                objective_function.penScale = penalty_scale_list[idx]
            objective_function.fail = failure_val_list[idx]
            problem.obj.append(objective_function)
            problem.objtype = problem.OBJ_TYPE_EVAL
        
        return problem

    def create_problem_objective_multiple (self, problem, objectives_list, output_nodes_list, failure_val_list, penalty_scale_list, weights_list):
    
        """
        This function creates a problem objective
        
        Parameters
        ----------
        problem : foqus_lib.framework.optimizer.problem
            The problem object.
        objectives_list : list
            The list of objective names.
            example: ['GWP', 'CED']
        output_nodes_list : list
            The list of output nodes.
            example: [nf.olca_node, nf.prommis_node]
            Name of the node whose outputs are used as objectives (e.g. "openLCA" for GWP/CED).
        failure_val_list : list
            The failure value for each objective.
            example: [10, 10]
            Note: the failure value should be higher than the expected highest value for a successful objective.
        penalty_scale_list : list
            The penalty scale for each objective.

        Returns
        -------
        problem : foqus_lib.framework.optimizer.problem
            The problem object.
        """
        problem.obj = []
        
        # generate obj py_code
        obj_py_code = ""

        for idx, obj in enumerate(objectives_list):
            term = (
                f'(f["{output_nodes_list[idx].name}"]["{obj}"]'
                f' * {float(weights_list[idx])}'
                f' / {float(penalty_scale_list[idx])})'
            )

            # add '+' between terms
            obj_py_code += (" + " if idx > 0 else "") + term
        
        # generate failure value
        failure_value = 0
        for idx, obj in enumerate(objectives_list):
            failure_value += (
                failure_val_list[idx] * 
                weights_list[idx] / 
                penalty_scale_list[idx]
            ) 

        for idx, obj in enumerate(objectives_list):

            objective_function = objectiveFunction()
            objective_function.pycode = obj_py_code
            objective_function.penScale = 1
            objective_function.fail = failure_value
            problem.obj.append(objective_function)
            problem.objtype = problem.OBJ_TYPE_EVAL
        
        return problem

    def get_max_constraint (self, variable, node, max_value):
        """
        Helper method to generate a inquality constraint function of the form: g(x) <= 0
        The constraint in this case is to set a maximum value for a variable.
        Example: function to set a constraint where maximum water consumption is 1000 Liters

        Parameters
        ----------
        variable : str
            The name of the variable to set the maximum value for.
        node : str
            The name of the node that contains the variable.
        max_value : float
            The maximum value to set for the variable.

        Returns
        -------
        str
            The python code to be used as a constraint.
        """
        return f'(f["{node.name}"]["{variable}"] - {max_value})'

    def get_min_constraint (self, variable, node, min_value):
        """
        Helper method to generate a inquality constraint function of the form: g(x) <= 0
        The constraint in this case is to set a minimum value for a variable.
        Example: function to set a constraint where minimum recovery rate is 90% (e.g., 0.9)

        Parameters
        ----------
        variable : str
            The name of the variable to set the maximum value for.
        node : str
            The name of the node that contains the variable.
        min_value : float
            The minimum value to set for the variable.

        Returns
        -------
        str
            The python code to be used as a constraint.
        """

        return f'({min_value} - f["{node.name}"]["{variable}"])'

    def create_problem_constraint ( self, 
                                    problem, 
                                    pycode,
                                    penalty_factor=10,
                                    form = "Linear"):
        """
        This function creates a problem constraint and appends it to the problem

        Parameters
        ----------
        problem : foqus_lib.framework.optimizer.problem
            The problem object.
        pycode : str
            The python code to be used as a constraint.
        penalty_factor : float
            The penalty scale factor (default: 10).
        form : str
            The form of the constraint (default: "Linear").

        Returns
        -------
        problem : foqus_lib.framework.optimizer.problem
            The problem object.
        """

        forms = [None, "Linear", "Quadratic", "Step"]

        const = inequalityConstraint()
        const.pycode = pycode
        const.penalty = penalty_factor
        const.penForm = form

        problem.g.append(const)

        return problem

    def setup_nlopt_solver_options(self, 
                            problem, 
                            use_defaults = False, 
                            algorithm = None, 
                            max_func_eval = None, 
                            max_time = None, 
                            tol_func_abs = None,
                            tol_x_abs = None,
                            tol_x_rel = None,
                            tol_func_rel = None, 
                            lower_bound = None, 
                            upper_bound = None):

        """
        This function setups the solver options 
        The user has the option to simply use the default solver options
        or to specify the solver options manually
        Parameters
        ----------
        use_defaults : bool
            Whether to use the default solver options.
        algorithm : str
            The algorithm to use.
        max_func_eval : int
            The maximum number of function evaluations.
        max_time : float
            The maximum time in hours.
        tol_func_abs : float
            The absolute tolerance for the function.
        tol_x_abs : float
            The absolute tolerance for the variables.
        tol_x_rel : float
            The relative tolerance for the variables.
        tol_func_rel : float
            The relative tolerance for the function.
        lower_bound : float
            The lower bound.
        upper_bound : float
            The upper bound.

        Returns
        -------
        problem : foqus_lib.framework.optimizer.problem
            The problem object.
        """

        if use_defaults:
            problem.solverOptions[problem.solver] = {
            "Solver": "BOBYQA",   
            "maxeval": 0,       
            "maxtime": 60,
            "tolfunabs": 1e-9,    
            "tolfunrel": 1e-9,
            "tolxabs": 1e-9,
            "tolxrel": 1e-9,
            "lower": 0,         
            "upper": 10    
            }
        else:
            optim = nlopt.opt()
            valid_v = optim.options["Solver"].validValues
            if algorithm not in valid_v:
                raise ValueError(f"Invalid algorithm: {algorithm}. Valid algorithms are: {valid_v}")
            problem.solverOptions[problem.solver] = {
                "Solver": algorithm,
                "maxeval": max_func_eval,
                "maxtime": max_time,
                "tolfunabs": tol_func_abs,
                "tolfunrel": tol_func_rel,
                "tolxabs": tol_x_abs,
                "tolxrel": tol_x_rel,
                "lower": lower_bound,
                "upper": upper_bound
            }

        return problem

    def run_optimization(self, problem, session):
        """
        This function runs the optimization with validation

        Parameters
        ----------
        problem : foqus_lib.framework.optimizer.problem
            The problem object.
        session : foqus_lib.framework.session.session
            The session object.

        Returns
        -------
        solver : foqus_lib.framework.optimizer.optimization
            The solver object.
        problem : foqus_lib.framework.optimizer.problem
            The problem object.
        """
        
        my_solver = problem.run(session)
        my_solver.join()  # wait for results
        logging.info("Optimization completed")

        return my_solver, problem

###############################################################################
# NODE SCRIPTS
###############################################################################
openlca_node_script = """
# import dependencies
from pathlib import Path
import pandas as pd
import olca_schema as olca
import re
import os

from netlolca.NetlOlca import NetlOlca

import src as lca_prommis

output_dir = lca_prommis.setup_output_directory(
            lca_prommis.output_dir
            )

# get the parameters file from the output directory
my_parameters_path = os.path.join(output_dir, "my_parameters.csv")
my_parameters = pd.read_csv(my_parameters_path)
params = my_parameters.copy() # get a copy of the parameters df

# update the parameters table with the new parameter values
existing = [
    int(m.group(1))
    for col in params.columns
    if (m := re.match(r"parameter_value_(\\d+)", col))
]
new_col = f"parameter_value_{max(existing, default=0) + 1}"
# update the parameters table with the new parameter values
for count, row in params.iterrows():
    desc = row['parameter_description']

    matching_keys = []
    for k in x.keys():
        if k.startswith(desc):
            matching_keys.append(k)

    if matching_keys and x[matching_keys[0]] is not None:
        params.at[count, new_col] = float(x[matching_keys[0]])
    else:
        params.at[count, new_col] = 0

# save/overwrite the updated parameters table to the output directory
params.to_csv(my_parameters_path, index=False)

params1 = params.copy()
params1 = params1[['parameter_name', 'parameter_description', new_col]]
params1.rename(columns={new_col: 'parameter_value'}, inplace=True)

# get run information - already initiated outside the olca_node_script
run_info_path = os.path.join(output_dir, "run_info.csv")
run_info = pd.read_csv(run_info_path)
ps_uuid = run_info.loc[run_info['item'] == 'ps_uuid', 'description'].values[0]
impact_method_uuid = run_info.loc[run_info['item'] == 'impact_method_uuid', 'description'].values[0]
parameter_set_name = run_info.loc[run_info['item'] == 'parameter_set_name', 'description'].values[0]

# connect to openLCA
netl = NetlOlca()
netl.connect()
netl.read()

param_set_ref = lca_prommis.run_analysis.update_parameter ( netl, 
                                                            ps_uuid = ps_uuid,
                                                            parameter_set_name = parameter_set_name, 
                                                            new_parameter_set = params1) 

result = lca_prommis.run_analysis.run_analysis (netl, 
                                                ps_uuid = ps_uuid, 
                                                impact_method_uuid = impact_method_uuid, 
                                                parameter_set = param_set_ref.parameters)
result.wait_until_ready()
total_impacts = lca_prommis.generate_total_results.generate_total_results(result)

# save the total impacts to the node outputs
for _, row in total_impacts.iterrows():
    f[row['name']] = row['amount']

impacts_path = os.path.join(output_dir, "total_impacts.csv")
if not os.path.exists(impacts_path):
    total_impacts.to_csv(impacts_path, index=False)
else:
    impacts = pd.read_csv(impacts_path)
    amount_cols = [
        col for col in impacts.columns
        if re.fullmatch(r'amount_\\d+', col)
    ]
    if amount_cols:
        last_n = max(int(col.split('_')[1]) for col in amount_cols)
        next_col = f'amount_{last_n + 1}'
    else:
        next_col = 'amount_1'
    impacts [next_col] = total_impacts['amount'].to_numpy()
    impacts.to_csv(impacts_path, index=False)
"""

prommis_node_script = """
import os
import re
import logging
globals()["re"] = re
import pandas as pd
import olca_schema as olca
from netlolca.NetlOlca import NetlOlca
import prommis.uky.uky_flowsheet as uky
import src as lca_prommis

from pyomo.environ import TransformationFactory, value
from idaes.core.util.model_diagnostics import DiagnosticsToolbox
from idaes.core.util.model_statistics import degrees_of_freedom
from prommis.uky.costing.ree_plant_capcost import QGESSCostingData
from prommis.uky.uky_flowsheet import display_costing
from idaes.core.scaling import AutoScaler

output_dir = lca_prommis.setup_output_directory(
            lca_prommis.output_dir
            )

m = uky.build()

uky.set_operating_conditions(m)

if "fs.leach_liquid_feed.flow_vol" in x:
    m.fs.leach_liquid_feed.flow_vol.fix(x["fs.leach_liquid_feed.flow_vol"])
    logging.info("Leach liquid feed: %f", x["fs.leach_liquid_feed.flow_vol"])
else:
    print ("Leach liquid feed not found in x")

if "fs.load_sep.split_fraction" in x:
    m.fs.load_sep.split_fraction[:, 'recycle'].fix(x["fs.load_sep.split_fraction"])
    logging.info("split_fraction: %f", x["fs.load_sep.split_fraction"])
else:
    print ("fs.load_sep.split_fraction not found in x")

# store new decision variable values in the output folder
dv_df = pd.read_csv(os.path.join(output_dir, "decision_variables.csv"))
# get new col name
existing = [
    int(n.group(1))
    for col in dv_df.columns
    if (n := re.match(r"value_(\\d+)", col))
]
new_col = f"value_{max(existing, default=0) + 1}"
for count, row in dv_df.iterrows():
    dv_name = row['variable_name']
    if dv_name in x:
        dv_df.at[count, new_col] = x[dv_name]
dv_df.to_csv(os.path.join(output_dir, "decision_variables.csv"), index = False)

uky.set_scaling(m)

if degrees_of_freedom(m) != 0:
    raise AssertionError("Degrees of freedom != 0")

uky.initialize_system(m)
uky.solve_system(m)

uky.fix_organic_recycle(m)
results = uky.solve_system(m)

if not uky.check_optimal_termination(results):
    raise RuntimeError("Solver failed to terminate optimally")

# Add result expressions (overall_ree_recovery_percentage, ree_product_purity_percentage, etc.)
uky.add_result_expressions(m)

# 5. Add Costing (Optional, but likely needed for optimization)
uky.add_costing(m)
uky.initialize_costing(m)

# diagnostics, initialize, and solve
dt = DiagnosticsToolbox(m)
dt.assert_no_structural_warnings()

auto = AutoScaler()
auto.scale_variables_by_magnitude(m)
auto.scale_constraints_by_jacobian_norm(m)

# Costing initialization
QGESSCostingData.costing_initialization(m.fs.costing)
QGESSCostingData.initialize_fixed_OM_costs(m.fs.costing)
QGESSCostingData.initialize_variable_OM_costs(m.fs.costing)

# Final solve with costing
uky.solve_system(m)

dt.assert_no_numerical_warnings()

display_costing(m)

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

finalized_df.to_csv(os.path.join(output_dir, "finalized_df.csv"), index=False)

for _, row in finalized_df.iterrows():
    f[row['Flow_Name']] = row['LCA_Amount']

prommis_outputs = { "total plant cost": value(m.fs.costing.total_overnight_capital),
                    "total bare erected cost": value(m.fs.costing.total_BEC),
                    "total annualized capital cost": value(m.fs.costing.annualized_cost),
                    "total fixed OM cost": value(m.fs.costing.total_fixed_OM_cost),
                    "total variable OM cost": value(m.fs.costing.total_variable_OM_cost[0]),
                    "total OM cost": value(m.fs.costing.total_fixed_OM_cost) + value(m.fs.costing.total_variable_OM_cost[0]),
                    "total annualized plant cost": value(m.fs.costing.annualized_cost) + value(m.fs.costing.total_fixed_OM_cost) + value(m.fs.costing.total_variable_OM_cost[0]),
                    "anual rate of recovery": value(m.fs.costing.recovery_rate_per_year),
                    "cost of recovery per REE": value(m.fs.costing.cost_of_recovery),
                    "recovery rate": value(m.fs.overall_ree_recovery_percentage[0]),
                    "product purity": value(m.fs.ree_product_purity_percentage[0])
} 

for output, val in prommis_outputs.items():
    f[output] = val

# store new results values in the output folder
prommis_outputs = pd.read_csv(os.path.join(output_dir, "prommis_outputs.csv"))
# get new col name
existing = [
    int(n.group(1))
    for col in prommis_outputs.columns
    if (n := re.match(r"value_(\\d+)", col))
]
new_col = f"value_{max(existing, default=0) + 1}"
for count, row in prommis_outputs.iterrows():
    output = row['output']
    prommis_outputs.loc[count, new_col] = f[output] #new value for the output
prommis_outputs.to_csv(os.path.join(output_dir, "prommis_outputs.csv"), index = False)
"""

###############################################################################
# FUNCTIONS
###############################################################################
def get_uky_vars_exchanges():
    """Run the UKy flowsheet and process LCA data to create an initial
    exchange table.

    Returns
    -------
    pandas.DataFrame
    """
    # Build the ConcreteModel from UKy flowsheet
    m, _ = uky.main()

    # Extract all potential PrOMMiS variables
    all_vars = []
    for my_var in m.component_objects(Var):
        all_vars.append(my_var.name)

    # Create LCA exchanges (for PrOMMiS outputs to openLCA inputs)
    prommis_data = lca_prommis.data_lca.get_lca_df(m)

    df = lca_prommis.convert_lca.convert_flows_to_lca_units(
        prommis_data,
        hours=1,
        mol_to_kg=True,
        water_unit='m3'
    )

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

    df = lca_prommis.final_lca.merge_flows(
        df,
        merge_source='Solid Feed',
        new_flow_name='374 ppm REO Feed',
        value_2_merge=REO_list
    )

    df = lca_prommis.final_lca.merge_flows(
        df,
        merge_source='Roaster Product',
        new_flow_name='73.4% REO Product'
    )

    df = lca_prommis.final_lca.merge_flows(
        df,
        merge_source='Wastewater',
        new_flow_name='Wastewater',
        merge_column='Category'
    )

    df = lca_prommis.final_lca.merge_flows(
        df,
        merge_source='Solid Waste',
        new_flow_name='Solid Waste',
        merge_column='Category'
    )

    finalized_df = lca_prommis.final_lca.finalize_df(
        df=df,
        reference_flow='73.4% REO Product',
        reference_source='Roaster Product',
        water_type='raw fresh water'
    )
    return (all_vars, finalized_df, m)

def initiate_lca_model(client, 
                        process_name, 
                        process_description, 
                        lca_df_finalized, 
                        impact_method_uuid, 
                        parameter_set_name, 
                        parameter_set_description, 
                        is_baseline,
                        save_outputs=False,
                        output_dir = None):
    """
    Initiate the LCA model.
    Parameters
    ----------
    client : olca_ipc.Client
        The IPC client object.
    process_name : str
        The name of the process.
    process_description : str
        The description of the process.
    lca_df_finalized : pandas.DataFrame
        The finalized LCA dataframe.
    impact_method_uuid : str
        The UUID of the impact method.
    save_outputs : bool
        Whether to save the outputs to the output directory.
    output_dir : str
        The directory to save the outputs.

    Returns
    -------
    total_impacts : dict
        The total impacts of the process.
    my_parameters : list
        The parameters of the process.
    ps_uuid : str
        The UUID of the product system.
    """
    if save_outputs:
        if output_dir is None:
            raise ValueError("Please provide an output directory to save the outputs")
    
    process, my_parameters = lca_prommis.create_lca.create_new_process(client,
                                                                        lca_df_finalized,
                                                                        process_name,
                                                                        process_description)
    ps = lca_prommis.create_ps.create_ps(client, process.id)
    ps_uuid = ps.id
    # create baseline parameter set
    parameter_set = lca_prommis.run_analysis.create_parameter_set(client, 
                                                                process.id, 
                                                                ps.id, 
                                                                parameter_set_name, 
                                                                parameter_set_description, 
                                                                is_baseline)
    
    result = lca_prommis.run_analysis.run_analysis(client, 
                                                    ps.id, 
                                                    impact_method_uuid, 
                                                    parameter_set.parameters)
    result.wait_until_ready()
    total_impacts = lca_prommis.generate_total_results.generate_total_results(result)

    if save_outputs:
        total_impacts.to_csv(os.path.join(output_dir, "total_impacts.csv"), index=False)
        my_parameters.to_csv(os.path.join(output_dir, "my_parameters.csv"), index=False)

        run_info = pd.DataFrame(columns=['item', 'description'])
        run_info.loc[len(run_info)] = ['ps_uuid', ps_uuid]
        run_info.loc[len(run_info)] = ['impact_method_uuid', impact_method_uuid]
        run_info.loc[len(run_info)] = ['parameter_set_name', parameter_set_name]
        run_info.to_csv(os.path.join(output_dir, "run_info.csv"), index=False)
    
    return total_impacts, my_parameters, ps_uuid

def initialize_decision_variables(nf_obj, m):
    """
    Initialize the decision variables.
    Parameters
    ----------
    nf_obj : NetlFoqus
        The NetlFoqus object.
    m : pyomo.environ.ConcreteModel
        The model.

    Returns
    -------
    None
    """
    for dv in nf_obj.dv:
        # Execution string to get current value from model
        my_str = "m.%s.get_values().values()" % dv.ipvname
        my_val = [x for x in eval(my_str)][0]
        # Use +- 10% for max and min
        my_min = my_val * 0.9
        my_max = my_val * 1.1
        # Set decision variable properties
        dv.setValue(my_val)
        dv.setMin(my_min)
        dv.setMax(my_max)
        # Linear scaling maps [min, max] -> [0, 10] so the NLopt initial point
        # (scaled=True) lies inside bounds [lower, upper]; required by BOBYQA.
        dv.scaling = "Linear"
        # NOTE: distribution is uniform by default; can be changed later
        logging.info(
            "Set decision properties for %s: value=%f, min=%f, max=%f" % (
                dv.ipvname, my_val, my_min, my_max
            )
        )

def validate_optimization_problem(problem, session): # Work still in progress - See TODOs at line 1300
    """
    Validate the optimization problem before running
    """
    logging.info(f"Decision Variables (v): {problem.v}")
    logging.info(f"Number of Decision Variables: {len(problem.v)}")
    
    # Check if variables are properly defined
    if len(problem.v) == 0:
        raise ValueError("No decision variables defined in optimization problem!")
    
    # Check bounds
    logging.info(f"Variable bounds: {problem.v}")
    
    # Check objective function
    logging.info(f"Objective: {problem.obj}")
    if len(problem.obj) == 0:
        raise ValueError("No objective function defined!")
    
    # Check solver settings
    if problem.solver is None:
        raise ValueError("No solver selected!")
    
    logging.info(f"Selected Solver: {problem.solver}")
    logging.info(f"Solver Options: {problem.solverOptions.get(problem.solver, {})}")

def generate_penalty_scales(prommis_outputs_df, olca_outputs_df):
    """
    Helper method to generate penalty scales for the potential objective variables
    The penalty scales are calculated as the inverse of the initial values of the objective variables
    This helps the solver normalize the values

    Parameters
    ----------
    prommis_outputs_df : pandas.DataFrame
        The dataframe containing the prommis outputs from the first run
    olca_outputs_df : pandas.DataFrame
        The dataframe containing the olca outputs from the first run

    Returns
    -------
    ps_guide : pandas.DataFrame
        The dataframe containing the penalty scales for the potential objective variables
    
    Notes
    -----
    * This method assumes that the objective variables don't range across orders of magnitude in the 
    subsequent runs
    """
    ps_guide = pd.DataFrame(columns=['objective', 'initial_value', 'penalty_scale'])
    ps_guide[['objective', 'initial_value']] = prommis_outputs_df[['output', 'value']].values
    ps_guide = pd.concat(
        [ps_guide,
        olca_outputs_df[['name', 'amount']].rename(columns={'name': 'objective', 'amount': 'initial_value'})
        ],
        ignore_index=True
    )
    for idx, row in ps_guide.iterrows():
        ps_guide.at[idx, 'penalty_scale'] = 1/row['initial_value'] if row['initial_value'] != 0 else 1    

    return ps_guide

def get_penalty_scales(objectives_list, ps_guide):
    """
    Helper method to get the penalty scales based on a selection of objectives

    Parameters
    ----------
    objectives_list : list
        The list of objective names.
    ps_guide : pandas.DataFrame
        The dataframe containing the penalty scales for the potential objective variables

    Returns
    -------
    penalty_scale_list : list
        The list of penalty scales for the selected objectives
    """
    penalty_scale_list = []

    for objective in objectives_list:
        if objective not in ps_guide['objective'].to_list():
            raise ValueError(f"Objective {objective} not found in ps_guide")
        penalty_scale_list.append(ps_guide.loc[ps_guide['objective'] == objective, 'penalty_scale'].values[0])

    return penalty_scale_list

def export_decision_variables(nf_obj):
    dv_data = []
    for dv in nf_obj.dv:
        dv_data.append({
            'variable_name': dv.ipvname,
            'value': dv.value,
            'min': dv.min,
            'max': dv.max,
            'scaling': dv.scaling
        })

    dv_df = pd.DataFrame(dv_data)
    dv_df.to_csv(os.path.join(nf_obj.output_dir, "decision_variables.csv"), index=False)

    return dv_df

def generate_prommis_outputs(nf_obj, m):
    from pyomo.environ import value
    prommis_outputs = { "total plant cost": value(m.fs.costing.total_overnight_capital),
                        "total bare erected cost": value(m.fs.costing.total_BEC),
                        "total annualized capital cost": value(m.fs.costing.annualized_cost),
                        "total fixed OM cost": value(m.fs.costing.total_fixed_OM_cost),
                        "total variable OM cost": value(m.fs.costing.total_variable_OM_cost[0]),
                        "total OM cost": value(m.fs.costing.total_fixed_OM_cost) + value(m.fs.costing.total_variable_OM_cost[0]),
                        "total annualized plant cost": value(m.fs.costing.annualized_cost) + value(m.fs.costing.total_fixed_OM_cost) + value(m.fs.costing.total_variable_OM_cost[0]),
                        "anual rate of recovery": value(m.fs.costing.recovery_rate_per_year),
                        "cost of recovery per REE": value(m.fs.costing.cost_of_recovery),
                        "recovery rate": value(m.fs.overall_ree_recovery_percentage[0]),
                        "product purity": value(m.fs.ree_product_purity_percentage[0])
    }

    for output, value in prommis_outputs.items():
        nf_obj.initiate_output_variables(nf_obj.prommis_node,
                                        output,
                                        value)

    prommis_outputs_df = pd.DataFrame(prommis_outputs.items(),columns=["output", "value"])
    prommis_outputs_df.to_csv(os.path.join(nf_obj.output_dir, "prommis_outputs.csv"), index=False)

    return prommis_outputs_df

def get_foqus_wd():
    foqus_file = ".foqus.cfg"  # created by FOQUS installer
    foqus_path = os.path.join(os.path.expanduser("~"), foqus_file)
    foqus_wd = None
    if os.path.isfile(foqus_path):
        with open(foqus_path, 'r') as f:
            my_str = f.read()
        # The file is just a JSON, read as such into a dictionary
        my_dict = json.loads(my_str)
        foqus_wd = my_dict.get("working_dir", None)
    return foqus_wd

def create_openlca_outputs(nf_obj, total_impacts, node_name):
    """
    Creates the openLCA outputs for the given impact categories
    """
    for impact_category in total_impacts['name']:
        nf_obj.initiate_output_variables(node_name, 
                                    impact_category, 
                                    total_impacts.loc[total_impacts['name'] == impact_category, 'amount'].values[0])

def get_optimization_results(client, ps_uuid, parameter_set_name, solver, decision_variables, prommis_outputs, parameters, total_impacts):

    """
    This function:
    * Gets the optimization results from the solver
    * Identifies the best solution 
    * Exports and displays the results for the best solution inlcuding
        1- The decision variables
        2- The prommis outputs
        3- The openLCA exchanges values (parameters)
        4- The total environmental impact results

    Parameters
    ----------
    client : client
        The netlolca client object
    ps_uuid : str
        The UUID of the product system
    parameter_set_name : str
        The name of the parameter set
    solver : NLoptSolver
        The solver object containing the optimization results
    decision_variables : dataframe
        The decision variables
    prommis_outputs : dataframe
        The prommis outputs
    parameters : dataframe
        The openLCA exchanges values (parameters)
    total_impacts : dataframe
        The total environmental impact results
    """

    bestSoFarList = solver.bestSoFarList
    best_so_far_index = bestSoFarList[-1][0] + 1

    # get parameters for given index
    col_name_param = f"parameter_value_{best_so_far_index}"
    parameters_solution = parameters[['parameter_name', 'parameter_description', col_name_param]]
    parameters_solution = parameters_solution.rename(
    columns={col_name_param: 'parameter_value'}
    )    # pass the parameters to openLCA
    lca_prommis.run_analysis.update_parameter(client, 
                                              ps_uuid = ps_uuid,
                                              parameter_set_name = parameter_set_name, 
                                              new_parameter_set = parameters_solution) 
    # get environmental impact results for given index
    col_name_impacts = f"amount_{best_so_far_index}"
    total_impacts_solution = total_impacts[['name', 'units',col_name_impacts]]

    # get decision variables for given index
    col_name_dv = f"value_{best_so_far_index}"
    decision_variables_solution = decision_variables[['variable_name', col_name_dv]]

    # prommis outputs
    col_name_prommis = f"value_{best_so_far_index}"
    prommis_outputs_solution = prommis_outputs[['output', col_name_prommis]]

    return parameters_solution, total_impacts_solution, decision_variables_solution, prommis_outputs_solution


    
#
# SANDBOX
#
if __name__ == "__main__":
    # Import and initialize NetlFoqus w/ UKy flowsheet
    import src.foqus_class as foqus_class
    from src.foqus_class import NetlFoqus
    from src.foqus_class import openlca_node_script, prommis_node_script
    import os
    import logging
    from pathlib import Path
    import pandas as pd
    import olca_schema as olca
    from netlolca.NetlOlca import NetlOlca
    import prommis.uky.uky_flowsheet as uky
    from pyomo.environ import value
    from pyomo.core.base.var import Var
    import foqus_lib.framework.graph.graph as gr
    import foqus_lib.framework.graph.nodeVars as nv
    from foqus_lib.framework.uq.Distribution import Distribution
    from foqus_lib.framework.session.session import session
    from foqus_lib.framework.optimizer.problem import objectiveFunction, inequalityConstraint
    import src as lca_prommis
    import foqus_lib.framework.optimizer.NLopt as nlopt

    nf = NetlFoqus()
    m = nf.init_uky()

    # Add a decision variable
    my_var1 = "fs.leach_liquid_feed.flow_vol"
    nf.add_decision_variable(my_var1)

    # [FH] we need at least two decision variables
    my_var2 = "fs.load_sep.split_fraction"
    nf.add_decision_variable(my_var2)

    # Help with initializing decision variables
    foqus_class.initialize_decision_variables(nf, m)
    
    # Store decision variable information in a dataframe and save to output directory
    dv_data = []
    for dv in nf.dv:
        dv_data.append({
            'variable_name': dv.ipvname,
            'value': dv.value,
            'min': dv.min,
            'max': dv.max,
            'scaling': dv.scaling
        })

    dv_df = pd.DataFrame(dv_data)
    dv_df.to_csv(os.path.join(nf.output_dir, "decision_variables.csv"), index=False)

    # set decision variables as input variables for prommis_node
    for dv in nf.dv:
        nf.set_input_variables(nf.prommis_node, dv.ipvname, dv.value, dv.min, dv.max)

    # Initialize intermediate variables
    nf.initialize_intermediate_variables(nf.prommis_node, nf.olca_node)

    # connect intermediate variables
    nf.connect_intermediate_variables(nf.prommis_node, nf.olca_node)
    
    prommis_outputs = { "total plant cost": value(m.fs.costing.total_overnight_capital),
                "total bare erected cost": value(m.fs.costing.total_BEC),
                "total annualized capital cost": value(m.fs.costing.annualized_cost),
                "total fixed OM cost": value(m.fs.costing.total_fixed_OM_cost),
                "total variable OM cost": value(m.fs.costing.total_variable_OM_cost[0]),
                "total OM cost": value(m.fs.costing.total_fixed_OM_cost) + value(m.fs.costing.total_variable_OM_cost[0]),
                "total annualized plant cost": value(m.fs.costing.annualized_cost) + value(m.fs.costing.total_fixed_OM_cost) + value(m.fs.costing.total_variable_OM_cost[0]),
                "anual rate of recovery": value(m.fs.costing.recovery_rate_per_year),
                "cost of recovery per REE": value(m.fs.costing.cost_of_recovery),
                "recovery rate": value(m.fs.overall_ree_recovery_percentage[0]),
                "product purity": value(m.fs.ree_product_purity_percentage[0])
    }
    
    for output, value in prommis_outputs.items():
        nf.initiate_output_variables(nf.prommis_node,
                                     output,
                                     value)

    # export prommis outputs to the output directory
    prommis_outputs_df = pd.DataFrame(prommis_outputs.items(),columns=["output", "value"])
    prommis_outputs_df.to_csv(output_dir / "prommis_outputs.csv", index=False)
    

    # initiate lca_model
    lca_df_finalized = nf.exchanges
    netl = NetlOlca()
    netl.connect()
    netl.read()
    
    process_name = "TESTING - REO Extraction From Coal Mining Refuse | UKy Flowsheet"

    process_description = """This process involves the production of a Rare 
    Earth Oxide solid extraction from coal mining refuse. The scope of this 
    work starts with the leaching of size-reduced REE-rich feedstock (REE: 
    Rare Earth Elements) and ends with the recovery of mixed REO solids. The 
    process consists of six main stages: 1) Mixing and Leaching, 2) Rougher 
    Solvent Extraction, 3) Cleaner Solvent Extraction, 4) Precipitation, 
    5) Solid-Liquid (S/L) separation, and 6) Roasting. This process does not 
    account for upstream processes leading to the production of REE-rich 
    feedstock nor does it account for Downstream processes leading to the 
    separation of REE contained in the REO. The main product is a rare earth 
    oxide solid with no other by-products or co-products. The functional 
    unit is 1 kg of recovered REO solids. The material and energy inputs 
    shown in the system boundary figure below have been shortlisted and 
    estimated based on the UKy flowsheet output, as well as other relevant 
    literature."""

    impact_method_uuid = '60cb71ff-0ef0-4e6c-9ce7-c885d921dd15'

    parameter_set_name = "Baseline"
    parameter_set_description = "Baseline parameter set for the process"
    is_baseline = True
    total_impacts, my_parameters, ps_uuid = foqus_class.initiate_lca_model (netl, 
                                                                            process_name, 
                                                                            process_description, 
                                                                            lca_df_finalized, 
                                                                            impact_method_uuid, 
                                                                            parameter_set_name, 
                                                                            parameter_set_description, 
                                                                            is_baseline)
    

    # create new df/file to store run info
    run_info = pd.DataFrame(columns=['item', 'description'])
    run_info.loc[len(run_info)] = ['ps_uuid', ps_uuid]
    run_info.loc[len(run_info)] = ['impact_method_uuid', impact_method_uuid]
    run_info.loc[len(run_info)] = ['parameter_set_name', parameter_set_name]
    run_info.to_csv(output_dir / "run_info.csv", index=False)

    # create new df/file to store results
    total_impacts.to_csv(output_dir / "total_impacts.csv", index=False)

    # save my_parameters to the output directory
    my_parameters.to_csv(output_dir / "my_parameters.csv", index=False)

    # create output variables 
    for impact_category in total_impacts['name']:
        nf.initiate_output_variables(nf.olca_node, 
                                    impact_category, 
                                    total_impacts.loc[total_impacts['name'] == impact_category, 'amount'].values[0])
        logging.info(
            "Initiated output variable %s for node %s: amount=%f" % (
            impact_category, 
            nf.olca_node.name, 
            total_impacts.loc[total_impacts['name'] == impact_category, 'amount'].values[0]
            )
        )

    nf.define_node_script(nf.olca_node, openlca_node_script)

    # nf.validate_node_script(nf.olca_node)

    nf.define_node_script(nf.prommis_node, prommis_node_script)

    # nf.validate_node_script(nf.prommis_node)

    my_session = nf.create_session("/home/franc/foqus_wd") # create session
    # Note: This returns many warnings and errors - we can ignore this as we only 
    # care about foqus loading the NLopt solver
    
    problem = nf.setup_optimizer(my_session, "NLopt", nf.prommis_node) # first step in setting up optimizer

    ps_guide = foqus_class.generate_penalty_scales(prommis_outputs_df, total_impacts)

    objectives_list =  ["Freshwater ecotoxicity", "total plant cost"]
    
    penalty_scale_list = foqus_class.get_penalty_scales(objectives_list, ps_guide)

    # problem = nf.create_problem_objective_singular(problem,                                         
    #                                             ["Freshwater ecotoxicity"], 
    #                                             [nf.olca_node],
    #                                             [10000000],
    #                                             penalty_scale_list
    #                                             ) # create problem objective


    problem = nf.create_problem_objective_multiple(problem,                                         
                                                ["Freshwater ecotoxicity", "total plant cost"], 
                                                [nf.olca_node, nf.prommis_node],
                                                [10000000, 1.3],
                                                penalty_scale_list,
                                                [0.7, 0.3]
                                                ) # create problem objective

    # set up constraint
    #constraint = nf.

    problem = nf.setup_nlopt_solver_options(problem, True) # setup solver options

    my_solver, problem = nf.run_optimization(problem, my_session)  # run optimization

    # TODO:
    # 1.    create a function to extract the optimization result and pass the final                         --> In progress
    #       result to openLCA 

    # 2.    add code to extract and store the decision variables values at every run                        --> Done

    # 3.    create function to setup the problem constraint (alrady included above)                         --> Done
    #       Issue
    #       =====
    #       The issue with this method is that it requires the user to write a python code                  --> TBD
    #       to define the constraint - as such this is not ideal. 
    #       example: py_code = f["olca_node"]["Cumulative Energy Demand"] < 100
    #       What even makes this method more challenging is the need to have the constraint 
    #       variables included in the node outputs - which is autmatically true for the 
    #       olca_node but should be defined separately for the prommis_node.

    # 4.    fix function to validate the node script - the current function has bugs                        --> In progress

    # 5.    differentiate between water input and water emissions in lca_df_finalized                       --> Done

    # 6.    should the data extraction functionality be part of the netlfoqus class or                      --> TBD
    #       remain a separate code included in the jupter notebook and node scripts?

    # 7.    setup_optimizer: add error handling for solver name (should be checked for a list)              --> Skipped for now
    #                                                                                                           since we ony need NLopt

    # 8.    create_problem_objective: Add error handling for objectives list each objective                 --> Done 
    #       should be checked against the outputVars 

    # 9.    setup_nlopt_solver_options: Add error handling for algorithm each algorithm should              --> Done
    #       be checked for a list BOBYQA, COBYLA, DIRECT, etc.
    # 
    # 10.   Include 'value for failure' in objective setup function                                         --> Done
    #       The value for failure should be higher than the expected highest value for a successful 
    #       objective.

    # 11.   Objective Function setup
    #       11.1    adjust the create_problem_objective to allow the user to include multiple outputs in    --> Done
    #               a single scaled objective function.

    #       11.2    add code to export all the prommis model outputs and add them to the prommis node       --> Done
    #               outputs. These include: the recovery rate, the total mass output, the cost, etc.

    #       11.3    add code to store these prommis results in the outputs folder                           --> Done

    #       11.4    add code to create the ps_guide using the initation results of prommis (prommis         --> Done
    #               results) and openLCA (olca node outputs - e.g., impacts)

    #       11.5    test changes in sandbox                                                                 --> Done

    #       11.6    reflect changes in jupyter notebook                                                     --> In progress
