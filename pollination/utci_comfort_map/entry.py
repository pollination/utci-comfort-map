from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass
from typing import Dict, List

# pollination plugins and recipes
from pollination.honeybee_radiance.grid import MergeFolderData
from pollination.honeybee_radiance_postprocess.grid import MergeFolderData as MergeFolderDataPostProcess

from pollination.ladybug_comfort.map import MapResultInfo
from pollination.path.copy import Copy

# input/output alias
from pollination.alias.inputs.model import hbjson_model_grid_input
from pollination.alias.inputs.ddy import ddy_input
from pollination.alias.inputs.comfort import wind_speed_input, \
    utci_comfort_par_input, solar_body_par_indoor_input
from pollination.alias.inputs.north import north_input
from pollination.alias.inputs.runperiod import run_period_input
from pollination.alias.inputs.radiancepar import rad_par_annual_input
from pollination.alias.inputs.grid import min_sensor_count_input, cpu_count
from pollination.alias.inputs.schedule import comfort_schedule_csv_input
from pollination.alias.outputs.comfort import tcp_output, hsp_output, csp_output, \
    thermal_condition_output, utci_output, utci_category_output, env_conditions_output

from ._prepare_folder import PrepareFolder
from ._energy import EnergySimulation
from ._radiance import RadianceMappingEntryPoint
from ._view_factor import SphericalViewFactorEntryPoint
from ._dynshade import DynamicShadeContribEntryPoint
from ._comfort import ComfortMappingEntryPoint


@dataclass
class UtciComfortMapEntryPoint(DAG):
    """UTCI comfort map entry point."""

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

    cpu_count = Inputs.int(
        default=50,
        description='The maximum number of CPUs for parallel execution. This will be '
        'used to determine the number of sensors run by each worker.',
        spec={'type': 'integer', 'minimum': 1},
        alias=cpu_count
    )

    min_sensor_count = Inputs.int(
        description='The minimum number of sensors in each sensor grid after '
        'redistributing the sensors based on cpu_count. This value takes '
        'precedence over the cpu_count and can be used to ensure that '
        'the parallelization does not result in generating unnecessarily small '
        'sensor grids. The default value is set to 1, which means that the '
        'cpu_count is always respected.', default=500,
        spec={'type': 'integer', 'minimum': 1},
        alias=min_sensor_count_input
    )

    wind_speed = Inputs.file(
        description='A CSV file containing a single number for meteorological wind '
        'speed in m/s or several rows of wind speeds that align with the length of the '
        'run period. This will be used for all outdoor comfort evaluation. Note that '
        'all sensors on the indoors will always use a wind speed of 0.5 m/s, '
        'which is the lowest acceptable value for the UTCI model. If '
        'None, the EPW wind speed will be used for all outdoor sensors.',
        extensions=['txt', 'csv'], optional=True, alias=wind_speed_input
    )

    air_speed_matrices = Inputs.folder(
        description='An optional folder with csv files that align with the model '
        'sensor grids. Each csv file should have the same name as the sensor '
        'grid identifier. Each csv file should contain a matrix of air speed '
        'values in m/s with one row per sensor and one column per timestep of the run '
        'period. Note that these values are not meteorological and should be AT HUMAN '
        'SUBJECT LEVEL. If specified, this overrides the wind speed input.',
        optional=True
    )

    solarcal_parameters = Inputs.str(
        description='A SolarCalParameter string to customize the assumptions of '
        'the SolarCal model.', default='--posture standing --sharp 135 '
        '--absorptivity 0.7 --emissivity 0.95',
        alias=solar_body_par_indoor_input
    )

    comfort_parameters = Inputs.str(
        description='An UTCIParameter string to customize the assumptions of '
        'the UTCI comfort model.', default='--cold 9 --heat 26',
        alias=utci_comfort_par_input
    )

    schedule = Inputs.file(
        description='An optional path to a CSV file to specify the relevant times '
        'during which comfort should be evaluated. If specified, this will be used '
        'for all sensors. Values should be 0-1 separated by new line.',
        extensions=['txt', 'csv'], optional=True, alias=comfort_schedule_csv_input
    )

    radiance_parameters = Inputs.str(
        description='Radiance parameters for ray tracing.',
        default='-ab 2 -ad 5000 -lw 2e-05',
        alias=rad_par_annual_input
    )

    @task(template=PrepareFolder)
    def prepare_folder(
        self, model=model, epw=epw, north=north, run_period=run_period,
        cpu_count=cpu_count, min_sensor_count=min_sensor_count,
        air_speed_matrices=air_speed_matrices,
    ) -> List[Dict]:
        return [
            {
                'from': PrepareFolder()._outputs.results,
                'to': 'results'
            },
            {
                'from': PrepareFolder()._outputs.initial_results,
                'to': 'initial_results'
            },
            {
                'from': PrepareFolder()._outputs.metrics,
                'to': 'metrics'
            },
            {
                'from': PrepareFolder()._outputs.sensor_grids
            },
            {
                'from': PrepareFolder()._outputs.sensor_grids_folder,
                'to': 'radiance/grid'
            },
            {
                'from': PrepareFolder()._outputs.shortwave_resources,
                'to': 'radiance/shortwave/resources'
            },
            {
                'from': PrepareFolder()._outputs.longwave_resources,
                'to': 'radiance/longwave/resources'
            },
            {
                'from': PrepareFolder()._outputs.dynamic_shade_octrees
            }
        ]

    @task(template=EnergySimulation)
    def energy_simulation(
        self, model=model, epw=epw, ddy=ddy, north=north, run_period=run_period
    ) -> List[Dict]:
        return [
            {
                'from': EnergySimulation()._outputs.energy,
                'to': 'energy'
            }
        ]

    @task(
        template=SphericalViewFactorEntryPoint,
        needs=[prepare_folder],
        loop=prepare_folder._outputs.sensor_grids,
        sub_folder='radiance/view_factor/{{item.full_id}}',
        sub_paths={
            'octree_file_view_factor': 'scene.oct',
            'sensor_grid': '{{item.full_id}}.pts',
            'sky_dome': 'sky.dome',
            'sky_matrix': 'sky.mtx',
            'sky_matrix_direct': 'sky_direct.mtx',
            'sun_modifiers': 'suns.mod',
            'view_factor_modifiers': 'scene.mod'
        }
    )
    def run_spherical_view_factor_simulation(
        self,
        radiance_parameters=radiance_parameters,
        octree_file_view_factor=prepare_folder._outputs.longwave_resources,
        grid_name='{{item.full_id}}',
        sensor_grid=prepare_folder._outputs.sensor_grids_folder,
        sensor_count='{{item.count}}',
        sky_dome=prepare_folder._outputs.shortwave_resources,
        sky_matrix=prepare_folder._outputs.shortwave_resources,
        sky_matrix_direct=prepare_folder._outputs.shortwave_resources,
        sun_modifiers=prepare_folder._outputs.shortwave_resources,
        view_factor_modifiers=prepare_folder._outputs.longwave_resources
    ) -> List[Dict]:
        pass

    @task(
        template=RadianceMappingEntryPoint,
        needs=[prepare_folder],
        loop=prepare_folder._outputs.sensor_grids,
        sub_folder='radiance',
        sub_paths={
            'octree_file_with_suns': 'scene_with_suns.oct',
            'octree_file': 'scene.oct',
            'sensor_grid': '{{item.full_id}}.pts',
            'sky_dome': 'sky.dome',
            'sky_matrix': 'sky.mtx',
            'sky_matrix_direct': 'sky_direct.mtx',
            'sun_modifiers': 'suns.mod'
        }
    )
    def run_radiance_simulation(
        self,
        radiance_parameters=radiance_parameters,
        model=model,
        octree_file_with_suns=prepare_folder._outputs.shortwave_resources,
        octree_file=prepare_folder._outputs.shortwave_resources,
        grid_name='{{item.full_id}}',
        sensor_grid=prepare_folder._outputs.sensor_grids_folder,
        sensor_count='{{item.count}}',
        sky_dome=prepare_folder._outputs.shortwave_resources,
        sky_matrix=prepare_folder._outputs.shortwave_resources,
        sky_matrix_direct=prepare_folder._outputs.shortwave_resources,
        sun_modifiers=prepare_folder._outputs.shortwave_resources
    ) -> List[Dict]:
        return [
            {
                'from': RadianceMappingEntryPoint()._outputs.enclosures,
                'to': 'radiance/enclosures'
            },
            {
                'from': RadianceMappingEntryPoint()._outputs.shortwave_results,
                'to': 'radiance/shortwave/results'
            },
            {
                'from': RadianceMappingEntryPoint()._outputs.shortwave_grids,
                'to': 'radiance/shortwave/grids'
            }
        ]

    @task(
        template=DynamicShadeContribEntryPoint,
        needs=[prepare_folder],
        loop=prepare_folder._outputs.dynamic_shade_octrees,
        sub_folder='radiance',
        sub_paths={
            'octree_file': 'dynamic_shades/{{item.default}}',
            'octree_file_with_suns': 'dynamic_shades/{{item.sun}}',
            'sky_dome': 'sky.dome',
            'sky_matrix': 'sky.mtx',
            'sky_matrix_direct': 'sky_direct.mtx',
            'sun_modifiers': 'suns.mod',
            'sun_up_hours': 'sun-up-hours.txt',
            'sensor_grids': '_split_info.json'
        }
    )
    def run_radiance_dynamic_shade_contribution(
        self,
        radiance_parameters=radiance_parameters,
        octree_file=prepare_folder._outputs.shortwave_resources,
        octree_file_with_suns=prepare_folder._outputs.shortwave_resources,
        group_name='{{item.identifier}}',
        sensor_grid_folder='radiance/shortwave/grids',
        sensor_grids=prepare_folder._outputs.sensor_grids_folder,
        sky_dome=prepare_folder._outputs.shortwave_resources,
        sky_matrix=prepare_folder._outputs.shortwave_resources,
        sky_matrix_direct=prepare_folder._outputs.shortwave_resources,
        sun_modifiers=prepare_folder._outputs.shortwave_resources,
        sun_up_hours=prepare_folder._outputs.shortwave_resources,
    ) -> List[Dict]:
        pass

    @task(
        template=ComfortMappingEntryPoint,
        needs=[
            prepare_folder, energy_simulation, run_radiance_simulation,
            run_radiance_dynamic_shade_contribution,
            run_spherical_view_factor_simulation
        ],
        loop=prepare_folder._outputs.sensor_grids,
        sub_folder='initial_results',
        sub_paths={
            'result_sql': 'eplusout.sql',
            'enclosure_info': '{{item.full_id}}.json',
            'view_factors': '{{item.full_id}}.npy',
            'modifiers': 'scene.mod',
            'indirect_irradiance': '{{item.full_id}}.ill',
            'direct_irradiance': '{{item.full_id}}.ill',
            'ref_irradiance': '{{item.full_id}}.ill',
            'sun_up_hours': 'sun-up-hours.txt',
            'occ_schedules': 'occupancy_schedules.json',
            'trans_schedules': 'trans_schedules.json',
            'air_speed_mtx': 'conditions/{{item.full_id}}.csv'
        }
    )
    def run_comfort_map(
        self,
        epw=epw,
        result_sql=energy_simulation._outputs.energy,
        grid_name='{{item.full_id}}',
        enclosure_info='radiance/enclosures',
        view_factors='radiance/longwave/view_factors',
        modifiers=prepare_folder._outputs.longwave_resources,
        indirect_irradiance='radiance/shortwave/results/indirect',
        direct_irradiance='radiance/shortwave/results/direct',
        ref_irradiance='radiance/shortwave/results/reflected',
        sun_up_hours=prepare_folder._outputs.shortwave_resources,
        transmittance_contribs='radiance/shortwave/shd_trans/final/{{item.full_id}}',
        occ_schedules=prepare_folder._outputs.metrics,
        schedule=schedule,
        trans_schedules=prepare_folder._outputs.shortwave_resources,
        run_period=run_period,
        wind_speed=wind_speed,
        air_speed_mtx=prepare_folder._outputs.initial_results,
        solarcal_par=solarcal_parameters,
        comfort_parameters=comfort_parameters
    ) -> List[Dict]:
        return [
            {
                'from': ComfortMappingEntryPoint()._outputs.results_folder,
                'to': 'initial_results/results'
            },
            {
                'from': ComfortMappingEntryPoint()._outputs.conditions,
                'to': 'initial_results/conditions'
            },
            {
                'from': ComfortMappingEntryPoint()._outputs.metrics,
                'to': 'initial_results/metrics'
            }
        ]

    @task(template=MergeFolderDataPostProcess, needs=[run_comfort_map])
    def restructure_temperature_results(
        self, input_folder='initial_results/results/temperature', extension='csv'
    ):
        return [
            {
                'from': MergeFolderDataPostProcess()._outputs.output_folder,
                'to': 'results/temperature'
            }
        ]

    @task(template=MergeFolderDataPostProcess, needs=[run_comfort_map])
    def restructure_condition_results(
        self, input_folder='initial_results/results/condition', extension='csv'
    ):
        return [
            {
                'from': MergeFolderDataPostProcess()._outputs.output_folder,
                'to': 'results/condition'
            }
        ]

    @task(template=MergeFolderDataPostProcess, needs=[run_comfort_map])
    def restructure_condition_intensity_results(
        self, input_folder='initial_results/results/condition_intensity', extension='csv'
    ):
        return [
            {
                'from': MergeFolderDataPostProcess()._outputs.output_folder,
                'to': 'results/condition_intensity'
            }
        ]

    @task(template=MergeFolderData, needs=[run_comfort_map])
    def restructure_tcp_results(
        self, input_folder='initial_results/metrics/TCP', extension='csv'
    ):
        return [
            {
                'from': MergeFolderData()._outputs.output_folder,
                'to': 'metrics/TCP'
            }
        ]

    @task(template=MergeFolderData, needs=[run_comfort_map])
    def restructure_hsp_results(
        self, input_folder='initial_results/metrics/HSP', extension='csv'
    ):
        return [
            {
                'from': MergeFolderData()._outputs.output_folder,
                'to': 'metrics/HSP'
            }
        ]

    @task(template=MergeFolderData, needs=[run_comfort_map])
    def restructure_csp_results(
        self, input_folder='initial_results/metrics/CSP', extension='csv'
    ):
        return [
            {
                'from': MergeFolderData()._outputs.output_folder,
                'to': 'metrics/CSP'
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
            },
            {
                'from': MapResultInfo()._outputs.tcp_vis_metadata,
                'to': 'metrics/TCP/vis_metadata.json'
            },
            {
                'from': MapResultInfo()._outputs.hsp_vis_metadata,
                'to': 'metrics/HSP/vis_metadata.json'
            },
            {
                'from': MapResultInfo()._outputs.csp_vis_metadata,
                'to': 'metrics/CSP/vis_metadata.json'
            }
        ]

    @task(template=Copy, needs=[create_result_info])
    def copy_result_info(
        self, src=create_result_info._outputs.temperature_info
    ) -> List[Dict]:
        return [
            {
                'from': Copy()._outputs.dst,
                'to': 'initial_results/conditions/results_info.json'
            }
        ]

    # outputs
    environmental_conditions = Outputs.folder(
        source='initial_results/conditions',
        description='A folder containing the environmental conditions that were input '
        'to the thermal comfort model. This include the MRT, air temperature, longwave '
        'MRT, shortwave MRT delta, and relative humidity.', alias=env_conditions_output
    )

    utci = Outputs.folder(
        source='results/temperature', description='A folder containing CSV maps of '
        'Universal Thermal Climate Index (UTCI) temperatures for each sensor grid. '
        'Values are in Celsius.', alias=utci_output
    )

    condition = Outputs.folder(
        source='results/condition', description='A folder containing CSV maps of '
        'comfort conditions for each sensor grid. -1 indicates unacceptably cold '
        'conditions. +1 indicates unacceptably hot conditions. 0 indicates neutral '
        '(comfortable) conditions.', alias=thermal_condition_output
    )

    category = Outputs.folder(
        source='results/condition_intensity', description='A folder containing CSV maps '
        'of the heat/cold stress categories for each sensor grid. -5 indicates extreme '
        'cold stress. +5 indicates extreme heat stress. 0 indicates no thermal stress. '
        'This can be used to understand not just whether conditions are acceptable but '
        'how uncomfortably hot or cold they are.', alias=utci_category_output
    )

    tcp = Outputs.folder(
        source='metrics/TCP', description='A folder containing CSV values for Thermal '
        'Comfort Percent (TCP). TCP is the percentage of occupied time where '
        'thermal conditions are acceptable/comfortable. Note that outdoor sensors '
        'are considered always occupied.', alias=tcp_output
    )

    hsp = Outputs.folder(
        source='metrics/HSP', description='A folder containing CSV values for Heat '
        'Sensation Percent (HSP). HSP is the percentage of occupied time where '
        'thermal conditions are hotter than what is considered acceptable/comfortable.',
        alias=hsp_output
    )

    csp = Outputs.folder(
        source='metrics/CSP', description='A folder containing CSV values for Cold '
        'Sensation Percent (CSP). CSP is the percentage of occupied time where '
        'thermal conditions are colder than what is considered acceptable/comfortable.',
        alias=csp_output
    )
