from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from dataclasses import dataclass
from typing import Dict, List

# pollination plugins and recipes
from pollination.ladybug.translate import EpwToWea
from pollination.lbt_honeybee.edit import ModelModifiersFromConstructions

from pollination.honeybee_energy.translate import ModelOccSchedules, ModelTransSchedules

from pollination.honeybee_radiance.sun import CreateSunMatrix, ParseSunUpHours
from pollination.honeybee_radiance.translate import CreateRadianceFolderGrid
from pollination.honeybee_radiance.octree import CreateOctree, CreateOctreeWithSky
from pollination.honeybee_radiance.sky import CreateSkyDome, CreateSkyMatrix
from pollination.honeybee_radiance.grid import SplitGridFolder, SplitDataFolder
from pollination.honeybee_radiance.octree import CreateOctreeShadeTransmittance
from pollination.honeybee_radiance.viewfactor import ViewFactorModifiers

from pollination.path.copy import CopyMultiple

# input/output alias
from pollination.alias.inputs.model import hbjson_model_grid_input
from pollination.alias.inputs.north import north_input
from pollination.alias.inputs.runperiod import run_period_input
from pollination.alias.inputs.grid import min_sensor_count_input, cpu_count


@dataclass
class PrepareFolder(GroupedDAG):
    """Prepare folder for two phase daylight coefficient."""

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

    air_speed_matrices = Inputs.folder(
        description='An optional folder with csv files that align with the model '
        'sensor grids. Each csv file should have the same name as the sensor '
        'grid identifier. Each csv file should contain a matrix of air speed '
        'values in m/s with one row per sensor and one column per timestep of the run '
        'period. Note that these values are not meteorological and should be AT HUMAN '
        'SUBJECT LEVEL. If specified, this overrides the wind speed input.',
        optional=True
    )

    # tasks
    @task(template=EpwToWea)
    def create_wea(self, epw=epw, period=run_period) -> List[Dict]:
        return [
            {
                'from': EpwToWea()._outputs.wea,
                'to': 'radiance/shortwave/in.wea'
            }
        ]

    @task(template=CreateSunMatrix, needs=[create_wea])
    def generate_sunpath(self, north=north, wea=create_wea._outputs.wea, output_type=1):
        """Create sunpath for sun-up-hours."""
        return [
            {
                'from': CreateSunMatrix()._outputs.sunpath,
                'to': 'radiance/shortwave/resources/sunpath.mtx'
            },
            {
                'from': CreateSunMatrix()._outputs.sun_modifiers,
                'to': 'radiance/shortwave/resources/suns.mod'
            }
        ]

    @task(template=CreateSkyDome)
    def create_sky_dome(self):
        """Create sky dome for daylight coefficient studies."""
        return [
            {
                'from': CreateSkyDome()._outputs.sky_dome,
                'to': 'radiance/shortwave/resources/sky.dome'
            }
        ]

    @task(template=CreateSkyMatrix, needs=[create_wea])
    def create_total_sky(
        self, north=north, wea=create_wea._outputs.wea,
        sky_type='total', output_type='solar', sun_up_hours='sun-up-hours'
    ):
        return [
            {
                'from': CreateSkyMatrix()._outputs.sky_matrix,
                'to': 'radiance/shortwave/resources/sky.mtx'
            }
        ]

    @task(template=CreateSkyMatrix, needs=[create_wea])
    def create_direct_sky(
        self, north=north, wea=create_wea._outputs.wea,
        sky_type='sun-only', output_type='solar', sun_up_hours='sun-up-hours'
    ):
        return [
            {
                'from': CreateSkyMatrix()._outputs.sky_matrix,
                'to': 'radiance/shortwave/resources/sky_direct.mtx'
            }
        ]

    @task(template=ParseSunUpHours, needs=[generate_sunpath])
    def parse_sun_up_hours(self, sun_modifiers=generate_sunpath._outputs.sun_modifiers):
        return [
            {
                'from': ParseSunUpHours()._outputs.sun_up_hours,
                'to': 'radiance/shortwave/resources/sun-up-hours.txt'
            }
        ]

    @task(template=ModelModifiersFromConstructions)
    def set_modifiers_from_constructions(
        self, model=model, use_visible='solar', dynamic_behavior='static',
        exterior_offset=0.02
    ) -> List[Dict]:
        return [
            {
                'from': ModelModifiersFromConstructions()._outputs.new_model,
                'to': 'radiance/shortwave/model.hbjson'
            }
        ]

    @task(template=CreateRadianceFolderGrid, needs=[set_modifiers_from_constructions])
    def create_rad_folder(
        self, input_model=set_modifiers_from_constructions._outputs.new_model
    ):
        """Translate the input model to a radiance folder."""
        return [
            {
                'from': CreateRadianceFolderGrid()._outputs.model_folder,
                'to': 'radiance/shortwave/model'
            },
            {
                'from': CreateRadianceFolderGrid()._outputs.sensor_grids_file,
                'to': 'results/temperature/grids_info.json'
            }
        ]

    @task(template=CopyMultiple, needs=[create_rad_folder])
    def copy_grid_info(self, src=create_rad_folder._outputs.sensor_grids_file):
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
            },
            {
                'from': CopyMultiple()._outputs.dst_6,
                'to': 'initial_results/conditions/grids_info.json'
            }
        ]

    @task(
        template=SplitGridFolder, needs=[create_rad_folder],
        sub_paths={'input_folder': 'grid'}
    )
    def split_grid_folder(
        self, input_folder=create_rad_folder._outputs.model_folder,
        cpu_count=cpu_count, cpus_per_grid=3, min_sensor_count=min_sensor_count
    ):
        """Split sensor grid folder based on the number of CPUs"""
        return [
            {
                'from': SplitGridFolder()._outputs.output_folder,
                'to': 'radiance/grid'
            },
            {
                'from': SplitGridFolder()._outputs.dist_info,
                'to': 'initial_results/results/temperature/_redist_info.json'
            },
            {
                'from': SplitGridFolder()._outputs.sensor_grids_file,
                'to': 'radiance/grid/_split_info.json'
            }
        ]

    @task(template=CopyMultiple, needs=[split_grid_folder])
    def copy_redist_info(self, src=split_grid_folder._outputs.dist_info):
        return [
            {
                'from': CopyMultiple()._outputs.dst_1,
                'to': 'initial_results/results/condition/_redist_info.json'
            },
            {
                'from': CopyMultiple()._outputs.dst_2,
                'to': 'initial_results/results/condition_intensity/_redist_info.json'
            },
            {
                'from': CopyMultiple()._outputs.dst_3,
                'to': 'initial_results/metrics/TCP/_redist_info.json'
            },
            {
                'from': CopyMultiple()._outputs.dst_4,
                'to': 'initial_results/metrics/HSP/_redist_info.json'
            },
            {
                'from': CopyMultiple()._outputs.dst_5,
                'to': 'initial_results/metrics/CSP/_redist_info.json'
            },
            {
                'from': CopyMultiple()._outputs.dst_6,
                'to': 'initial_results/conditions/_redist_info.json'
            }
        ]

    @task(template=CreateOctree, needs=[create_rad_folder])
    def create_octree(self, model=create_rad_folder._outputs.model_folder):
        """Create octree from radiance folder."""
        return [
            {
                'from': CreateOctree()._outputs.scene_file,
                'to': 'radiance/shortwave/resources/scene.oct'
            }
        ]

    @task(
        template=CreateOctreeWithSky, needs=[generate_sunpath, create_rad_folder]
    )
    def create_octree_with_suns(
        self, model=create_rad_folder._outputs.model_folder,
        sky=generate_sunpath._outputs.sunpath
    ):
        """Create octree from radiance folder and sunpath for direct studies."""
        return [
            {
                'from': CreateOctreeWithSky()._outputs.scene_file,
                'to': 'radiance/shortwave/resources/scene_with_suns.oct'
            }
        ]

    @task(
        template=CreateOctreeShadeTransmittance,
        needs=[generate_sunpath, create_rad_folder]
    )
    def create_dynamic_shade_octrees(
        self, model=create_rad_folder._outputs.model_folder,
        sunpath=generate_sunpath._outputs.sunpath
    ):
        """Create a set of octrees for each dynamic window construction."""
        return [
            {
                'from': CreateOctreeShadeTransmittance()._outputs.scene_folder,
                'to': 'radiance/shortwave/resources/dynamic_shades'
            }
        ]

    @task(template=ModelTransSchedules)
    def create_model_trans_schedules(self, model=model, period=run_period) -> List[Dict]:
        return [
            {
                'from': ModelTransSchedules()._outputs.trans_schedule_json,
                'to': 'radiance/shortwave/resources/trans_schedules.json'
            }
        ]

    @task(template=ViewFactorModifiers)
    def create_view_factor_modifiers(
        self, model=model, include_sky='include', include_ground='include',
        grouped_shades='grouped'
    ):
        """Create octree from radiance folder and sunpath for direct studies."""
        return [
            {
                'from': ViewFactorModifiers()._outputs.modifiers_file,
                'to': 'radiance/longwave/resources/scene.mod'
            },
            {
                'from': ViewFactorModifiers()._outputs.scene_file,
                'to': 'radiance/longwave/resources/scene.oct'
            }
        ]

    @task(
        template=SplitDataFolder, needs=[create_rad_folder]
    )
    def split_air_speed_folder(
        self, input_folder=air_speed_matrices,
        grid_info_file=create_rad_folder._outputs.sensor_grids_file,
        cpu_count=cpu_count, cpus_per_grid=3, min_sensor_count=min_sensor_count,
        extension='.csv'
    ):
        """Split sensor grid folder based on the number of CPUs"""
        return [
            {
                'from': SplitDataFolder()._outputs.output_folder,
                'to': 'initial_results/conditions/air_speeds'
            }
        ]

    @task(template=ModelOccSchedules)
    def create_model_occ_schedules(self, model=model, period=run_period) -> List[Dict]:
        return [
            {
                'from': ModelOccSchedules()._outputs.occ_schedule_json,
                'to': 'metrics/occupancy_schedules.json'
            }
        ]

    results = Outputs.folder(
        source='results'
    )

    initial_results = Outputs.folder(
        source='initial_results'
    )

    metrics = Outputs.folder(
        source='metrics'
    )

    shortwave_resources = Outputs.folder(
        source='radiance/shortwave/resources', description='resources folder.'
    )

    longwave_resources = Outputs.folder(
        source='radiance/longwave/resources', description='resources folder.'
    )

    sensor_grids = Outputs.list(source='radiance/grid/_split_info.json')

    sensor_grids_folder = Outputs.folder(source='radiance/grid')

    dynamic_shade_octrees = Outputs.list(
        source='radiance/shortwave/resources/dynamic_shades/trans_info.json'
    )
