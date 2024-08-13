from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from dataclasses import dataclass
from typing import Dict, List

# pollination plugins and recipes
from pollination.honeybee_energy.settings import SimParComfort
from pollination.honeybee_energy.simulate import SimulateModelRoomBypass

# input/output alias
from pollination.alias.inputs.model import hbjson_model_grid_input
from pollination.alias.inputs.ddy import ddy_input
from pollination.alias.inputs.north import north_input
from pollination.alias.inputs.runperiod import run_period_input


@dataclass
class EnergySimulation(GroupedDAG):
    """Energy simulation."""

    # inputs
    model = Inputs.file(
        description='A Honeybee model in HBJSON file format.',
        extensions=['json', 'hbjson'],
        alias=hbjson_model_grid_input
    )

    epw = Inputs.file(
        description='EPW weather file to be used for the comfort map simulation.',
        extensions=['epw']
    )

    ddy = Inputs.file(
        description='A DDY file with design days to be used for the initial '
        'sizing calculation.', extensions=['ddy'],
        alias=ddy_input, optional=True
    )

    north = Inputs.float(
        default=0,
        description='A a number between -360 and 360 for the counterclockwise '
        'difference between the North and the positive Y-axis in degrees.',
        spec={'type': 'number', 'minimum': -360, 'maximum': 360},
        alias=north_input
    )

    run_period = Inputs.str(
        description='An AnalysisPeriod string to set the start and end dates of '
        'the simulation (eg. "6/21 to 9/21 between 0 and 23 @1"). If None, '
        'the simulation will be annual.', default='', alias=run_period_input
    )

    # tasks
    @task(template=SimParComfort)
    def create_sim_par(self, ddy=ddy, run_period=run_period, north=north) -> List[Dict]:
        return [
            {
                'from': SimParComfort()._outputs.sim_par_json,
                'to': 'energy/simulation_parameter.json'
            }
        ]

    @task(template=SimulateModelRoomBypass, needs=[create_sim_par])
    def run_energy_simulation(
        self, model=model, epw=epw,
        sim_par=create_sim_par._outputs.sim_par_json
    ) -> List[Dict]:
        return [
            {
                'from': SimulateModelRoomBypass()._outputs.sql,
                'to': 'energy/eplusout.sql'
            },
            {
                'from': SimulateModelRoomBypass()._outputs.idf,
                'to': 'energy/in.idf'
            }
        ]

    energy = Outputs.folder(source='energy')
