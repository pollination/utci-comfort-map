from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass
from typing import Dict, List

# pollination plugins and recipes
from pollination.ladybug.translate import EpwToWea
from pollination.ladybug_comfort.map import UtciMap, MapResultInfo, Tcp
from pollination.honeybee_radiance.translate import CreateRadiantEnclosureInfo
from pollination.honeybee_radiance.edit import MirrorModelSensorGrids
from pollination.honeybee_energy.settings import SimParComfort
from pollination.honeybee_energy.simulate import SimulateModel
from pollination.honeybee_energy.translate import ModelOccSchedules
from pollination.lbt_honeybee.edit import ModelModifiersFromConstructions
from pollination.annual_radiation.entry import AnnualRadiationEntryPoint
from pollination.path.copy import CopyMultiple

# input/output alias
from pollination.alias.inputs.model import hbjson_model_input
from pollination.alias.inputs.ddy import ddy_input
from pollination.alias.inputs.data import value_or_data
from pollination.alias.inputs.north import north_input
from pollination.alias.outputs.comfort import comfort_percent_output


@dataclass
class UTCIComfortMapEntryPoint(DAG):
    """UTCI comfort map entry point."""

    # inputs
    model = Inputs.file(
        description='A Honeybee model in HBJSON file format.',
        extensions=['json', 'hbjson'],
        alias=hbjson_model_input
    )

    epw = Inputs.file(
        description='EPW weather file to be used for the comfort map simulation.',
        extensions=['epw']
    )

    ddy = Inputs.file(
        description='A DDY file with design days to be used for the initial '
        'sizing calculation.', extensions=['ddy'],
        alias=ddy_input
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
        'the simulation will be annual.', default=''
    )

    sensor_count = Inputs.int(
        default=200,
        description='The maximum number of grid points per parallel execution.',
        spec={'type': 'integer', 'minimum': 1}
    )

    wind_speed = Inputs.str(
        description='A single number for meteorological wind speed in m/s or a string '
        'of a JSON array with numbers that align with the result-sql reporting period. '
        'This will be used for all indoor comfort evaluation while the EPW wind speed '
        'will be used for the outdoors.', default='0.5', alias=value_or_data
    )

    solarcal_parameters = Inputs.str(
        description='A SolarCalParameter string to customize the assumptions of '
        'the SolarCal model.', default='--posture seated --sharp 135 '
        '--absorptivity 0.7 --emissivity 0.95'
    )

    comfort_parameters = Inputs.str(
        description='An UTCIParameter string to customize the assumptions of '
        'the UTCI comfort model.', default='--cold 9 --heat 26'
    )

    radiance_parameters = Inputs.str(
        description='Radiance parameters for ray tracing.',
        default='-ab 2 -ad 5000 -lw 2e-05'
    )

    # tasks
    @task(template=EpwToWea)
    def create_wea(self, epw=epw, period=run_period) -> List[Dict]:
        return [
            {'from': EpwToWea()._outputs.wea,
             'to': 'in.wea'}
            ]

    @task(template=SimParComfort)
    def create_sim_par(self, ddy=ddy, run_period=run_period, north=north) -> List[Dict]:
        return [
            {'from': SimParComfort()._outputs.sim_par_json,
             'to': 'energy/simulation_parameter.json'}
            ]

    @task(template=SimulateModel, needs=[create_sim_par])
    def run_energy_simulation(
        self, model=model, epw=epw,
        sim_par=create_sim_par._outputs.sim_par_json
    ) -> List[Dict]:
        return [
            {'from': SimulateModel()._outputs.sql, 'to': 'energy/eplusout.sql'}
        ]

    @task(template=CreateRadiantEnclosureInfo)
    def get_enclosure_info(self, model=model) -> List[Dict]:
        return [
            {
                'from': CreateRadiantEnclosureInfo()._outputs.output_folder,
                'to': 'radiance/enclosures'
            },
            {
                'from': CreateRadiantEnclosureInfo()._outputs.enclosure_list_file,
                'to': 'results/temperature/grids_info.json'
            },
            {
                'from': CreateRadiantEnclosureInfo()._outputs.enclosure_list,
                'description': 'Information about exported enclosure JSONs.'
            }
        ]

    @task(template=CopyMultiple, needs=[get_enclosure_info])
    def copy_grid_info(self, src=get_enclosure_info._outputs.enclosure_list_file):
        return [
            {
                'from': CopyMultiple()._outputs.dst_1,
                'to': 'results/condition/grids_info.json'
            },
            {
                'from': CopyMultiple()._outputs.dst_2,
                'to': 'results/condition_intensity/grids_info.json'
            },
            {
                'from': CopyMultiple()._outputs.dst_3,
                'to': 'metrics/TCP/grids_info.json'
            },
            {
                'from': CopyMultiple()._outputs.dst_4,
                'to': 'metrics/HSP/grids_info.json'
            },
            {
                'from': CopyMultiple()._outputs.dst_5,
                'to': 'metrics/CSP/grids_info.json'
            }
        ]

    @task(template=ModelModifiersFromConstructions)
    def set_modifiers_from_constructions(
        self, model=model, use_visible='solar', exterior_offset=0.03
    ) -> List[Dict]:
        return [
            {'from': ModelModifiersFromConstructions()._outputs.new_model,
             'to': 'radiance/hbjson/1_energy_modifiers.hbjson'}
        ]

    @task(template=MirrorModelSensorGrids, needs=[set_modifiers_from_constructions])
    def mirror_sensor_grids(
        self, model=set_modifiers_from_constructions._outputs.new_model
    ) -> List[Dict]:
        return [
            {'from': MirrorModelSensorGrids()._outputs.new_model,
             'to': 'radiance/hbjson/2_mirrored_grids.hbjson'}
        ]

    @task(
        template=AnnualRadiationEntryPoint,
        needs=[create_wea, mirror_sensor_grids],
        sub_folder='radiance/shortwave',  # create a subfolder for the whole simulation
    )
    def run_irradiance_simulation(
        self, model=mirror_sensor_grids._outputs.new_model, wea=create_wea._outputs.wea,
        north=north, sensor_count=sensor_count, radiance_parameters=radiance_parameters
    ) -> List[Dict]:
        pass

    @task(
        template=UtciMap,
        needs=[run_energy_simulation, run_irradiance_simulation, get_enclosure_info],
        loop=get_enclosure_info._outputs.enclosure_list,
        sub_folder='results',  # create a subfolder for each grid
        sub_paths={
            'enclosure_info': '{{item.id}}.json',
            'total_irradiance': '{{item.id}}.ill',
            'direct_irradiance': '{{item.id}}.ill',
            'ref_irradiance': '{{item.id}}_ref.ill',
            'sun_up_hours': 'sun-up-hours.txt'
        }
    )
    def run_comfort_map(
        self, result_sql=run_energy_simulation._outputs.sql,
        enclosure_info=get_enclosure_info._outputs.output_folder, epw=epw,
        total_irradiance='radiance/shortwave/results/total',
        direct_irradiance='radiance/shortwave/results/direct',
        ref_irradiance='radiance/shortwave/results/total',
        sun_up_hours='radiance/shortwave/results/total',
        wind_speed=wind_speed, solarcal_par=solarcal_parameters,
        comfort_par=comfort_parameters, run_period=run_period
    ) -> List[Dict]:
        return [
            {
                'from': UtciMap()._outputs.temperature_map,
                'to': 'temperature/{{item.id}}.csv'
            },
            {
                'from': UtciMap()._outputs.condition_map,
                'to': 'condition/{{item.id}}.csv'
            },
            {
                'from': UtciMap()._outputs.category_map,
                'to': 'condition_intensity/{{item.id}}.csv'
            }
        ]

    @task(template=MapResultInfo)
    def create_result_info(
        self, comfort_model='utci', run_period=run_period
    ) -> List[Dict]:
        return [
            {
                'from': MapResultInfo()._outputs.temperature_info,
                'to': 'results/temperature/results_info.json'
            },
            {
                'from': MapResultInfo()._outputs.condition_info,
                'to': 'results/condition/results_info.json'
            },
            {
                'from': MapResultInfo()._outputs.condition_intensity_info,
                'to': 'results/condition_intensity/results_info.json'
            }
        ]

    @task(template=ModelOccSchedules)
    def create_model_occ_schedules(
        self, model=model, period=run_period
    ) -> List[Dict]:
        return [
            {'from': ModelOccSchedules()._outputs.occ_schedule_json,
             'to': 'metrics/occupancy_schedules.json'}
        ]

    @task(
        template=Tcp,
        needs=[create_model_occ_schedules, get_enclosure_info, run_comfort_map],
        loop=get_enclosure_info._outputs.enclosure_list,
        sub_folder='metrics',  # create a subfolder for the whole simulation
        sub_paths={
            'condition_csv': '{{item.id}}.csv',
            'enclosure_info': '{{item.id}}.json'
        }
    )
    def compute_tcp(
        self, condition_csv='results/condition',
        enclosure_info=get_enclosure_info._outputs.output_folder,
        occ_schedule_json=create_model_occ_schedules._outputs.occ_schedule_json
    ) -> List[Dict]:
        return [
            {'from': Tcp()._outputs.tcp, 'to': 'TCP/{{item.id}}.csv'},
            {'from': Tcp()._outputs.hsp, 'to': 'HSP/{{item.id}}.csv'},
            {'from': Tcp()._outputs.csp, 'to': 'CSP/{{item.id}}.csv'}
        ]

    # outputs
    results = Outputs.folder(
        source='results',
        description='A folder containing all results.'
    )

    utci = Outputs.folder(
        source='results/temperature', description='A folder containing CSV maps of '
        'Universal Thermal Climate Index (UTCI) temperatures for each sensor grid. '
        'Values are in Celsius.'
    )

    condition = Outputs.folder(
        source='results/condition', description='A folder containing CSV maps of '
        'comfort conditions for each sensor grid. -1 indicates unacceptably cold '
        'conditions. +1 indicates unacceptably hot conditions. 0 indicates neutral '
        '(comfortable) conditions.'
    )

    category = Outputs.folder(
        source='results/condition_intensity', description='A folder containing CSV maps '
        'of the heat/cold stress categories for each sensor grid. -5 indicates extreme '
        'cold stress. +5 indicates extreme heat stress. 0 indicates no thermal stress. '
        'This can be used to understand not just whether conditions are acceptable but '
        'how uncomfortably hot or cold they are.'
    )

    tcp = Outputs.folder(
        source='metrics/TCP', description='A folder containing CSV values for Thermal '
        'Comfort Percent (TCP). TCP is the percentage of occupied time where '
        'thermal conditions are acceptable/comfortable. Note that outdoor sensors '
        'are considered always occupied.', alias=comfort_percent_output
    )

    hsp = Outputs.folder(
        source='metrics/HSP', description='A folder containing CSV values for Heat '
        'Sensation Percent (HSP). HSP is the percentage of occupied time where '
        'thermal conditions are hotter than what is considered acceptable/comfortable.',
        alias=comfort_percent_output
    )

    csp = Outputs.folder(
        source='metrics/CSP', description='A folder containing CSV values for Cold '
        'Sensation Percent (CSP). CSP is the percentage of occupied time where '
        'thermal conditions are colder than what is considered acceptable/comfortable.',
        alias=comfort_percent_output
    )
