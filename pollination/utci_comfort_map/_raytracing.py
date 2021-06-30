"""Raytracing DAG for annual radiation."""

from pollination_dsl.dag import Inputs, DAG, task
from dataclasses import dataclass

from pollination.honeybee_radiance.grid import SplitGrid, MergeFiles
from pollination.honeybee_radiance.contrib import DaylightContribution
from pollination.honeybee_radiance.coefficient import DaylightCoefficient
from pollination.honeybee_radiance.sky import AddSkyMatrix


@dataclass
class AnnualIrradianceRayTracing(DAG):
    # inputs

    sensor_count = Inputs.int(
        default=200,
        description='The maximum number of grid points per parallel execution',
        spec={'type': 'integer', 'minimum': 1}
    )

    radiance_parameters = Inputs.str(
        description='The radiance parameters for ray tracing',
        default='-ab 2'
    )

    octree_file_with_suns = Inputs.file(
        description='A Radiance octree file with suns.',
        extensions=['oct']
    )

    octree_file = Inputs.file(
        description='A Radiance octree file.',
        extensions=['oct']
    )

    grid_name = Inputs.str(
        description='Sensor grid file name. This is useful to rename the final result '
        'file to {grid_name}.res'
    )

    sensor_grid = Inputs.file(
        description='Sensor grid file.',
        extensions=['pts']
    )

    sun_modifiers = Inputs.file(
        description='A file with sun modifiers.'
    )

    sky_matrix_indirect = Inputs.file(
        description='Path to indirect skymtx file (i.e. gendaymtx -s).'
    )

    sky_dome = Inputs.file(
        description='Path to sky dome file.'
    )

    bsdfs = Inputs.folder(
        description='Folder containing any BSDF files needed for ray tracing.',
        optional=True
    )

    @task(template=SplitGrid)
    def split_grid(self, sensor_count=sensor_count, input_grid=sensor_grid):
        return [
            {'from': SplitGrid()._outputs.grids_list},
            {'from': SplitGrid()._outputs.output_folder, 'to': '00_sub_grids'}
        ]

    @task(
        template=DaylightContribution, needs=[split_grid],
        loop=split_grid._outputs.grids_list, sub_folder='01_direct',
        sub_paths={'sensor_grid': '{{item.path}}'}
    )
    def direct_sunlight(
        self,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -ab 0 -dc 1.0 -dt 0.0 -dj 0.0 -dr 0',
        sensor_count='{{item.count}}', modifiers=sun_modifiers,
        sensor_grid=split_grid._outputs.output_folder,
        conversion='0.265 0.670 0.065',
        output_format='a',  # make it ascii so we can expose the file as a separate output
        scene_file=octree_file_with_suns,
        bsdf_folder=bsdfs
    ):
        return [
            {
                'from': DaylightContribution()._outputs.result_file,
                'to': '{{item.name}}.ill'
            }
        ]

    @task(
        template=DaylightCoefficient, needs=[split_grid],
        loop=split_grid._outputs.grids_list, sub_folder='02_indirect',
        sub_paths={'sensor_grid': '{{item.path}}'}
    )
    def indirect_sky(
        self,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -c 1',
        sensor_count='{{item.count}}',
        sky_matrix=sky_matrix_indirect, sky_dome=sky_dome,
        sensor_grid=split_grid._outputs.output_folder,
        conversion='0.265 0.670 0.065',  # divide by 179
        scene_file=octree_file,
        bsdf_folder=bsdfs
    ):
        return [
            {
                'from': DaylightContribution()._outputs.result_file,
                'to': '{{item.name}}.ill'
            }
        ]

    @task(
        template=AddSkyMatrix,
        needs=[split_grid, direct_sunlight, indirect_sky],
        loop=split_grid._outputs.grids_list, sub_folder='03_total'
    )
    def output_matrix_math(
        self,
        indirect_sky_matrix='02_indirect/{{item.name}}.ill',
        sunlight_matrix='01_direct/{{item.name}}.ill',

            ):
        return [
            {
                'from': AddSkyMatrix()._outputs.results_file,
                'to': '{{item.name}}.ill'
            }
        ]

    @task(
        template=MergeFiles, needs=[output_matrix_math]
    )
    def merge_total_results(self, name=grid_name, extension='.ill', folder='03_total'):
        return [
            {
                'from': MergeFiles()._outputs.result_file,
                'to': '../../results/total/{{self.name}}.ill'
            }
        ]

    @task(
        template=MergeFiles, needs=[output_matrix_math]
    )
    def merge_direct_results(
            self, name=grid_name, extension='.ill', folder='01_direct'):
        return [
            {
                'from': MergeFiles()._outputs.result_file,
                'to': '../../results/direct/{{self.name}}.ill'
            }
        ]
