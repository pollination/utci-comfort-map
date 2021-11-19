"""Raytracing DAG for annual radiation."""

from pollination_dsl.dag import Inputs, DAG, task
from dataclasses import dataclass

from pollination.honeybee_radiance.contrib import DaylightContribution
from pollination.honeybee_radiance.coefficient import DaylightCoefficient
from pollination.honeybee_radiance.sky import AddRemoveSkyMatrix


@dataclass
class AnnualIrradianceRayTracing(DAG):
    # inputs

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

    sensor_count = Inputs.int(
        description='Number of sensors in the input sensor grid.'
    )

    sun_modifiers = Inputs.file(
        description='A file with sun modifiers.'
    )

    sky_matrix = Inputs.file(
        description='Path to total sky matrix file.'
    )

    sky_matrix_direct = Inputs.file(
        description='Path to direct skymtx file (gendaymtx -d).'
    )

    sky_dome = Inputs.file(
        description='Path to sky dome file.'
    )

    bsdfs = Inputs.folder(
        description='Folder containing any BSDF files needed for ray tracing.',
        optional=True
    )

    @task(template=DaylightContribution)
    def direct_sun(
        self,
        name=grid_name,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -ab 0 -dc 1.0 -dt 0.0 -dj 0.0 -dr 0',
        sensor_count=sensor_count,
        modifiers=sun_modifiers,
        sensor_grid=sensor_grid,
        conversion='0.265 0.670 0.065',
        output_format='a',  # make it ascii so we expose the file as a separate output
        scene_file=octree_file_with_suns,
        bsdf_folder=bsdfs
    ):
        return [
            {
                'from': DaylightContribution()._outputs.result_file,
                'to': '../final/direct/{{self.name}}.ill'
            }
        ]

    @task(template=DaylightCoefficient)
    def direct_sky(
        self,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -ab 1 -c 1',
        sensor_count=sensor_count,
        sky_matrix=sky_matrix_direct, sky_dome=sky_dome,
        sensor_grid=sensor_grid,
        conversion='0.265 0.670 0.065',  # divide by 179
        scene_file=octree_file,
        bsdf_folder=bsdfs
    ):
        return [
            {
                'from': DaylightContribution()._outputs.result_file,
                'to': 'direct_sky.ill'
            }
        ]

    @task(template=DaylightCoefficient)
    def total_sky(
        self,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -c 1',
        sensor_count=sensor_count,
        sky_matrix=sky_matrix, sky_dome=sky_dome,
        sensor_grid=sensor_grid,
        conversion='0.265 0.670 0.065',  # divide by 179
        scene_file=octree_file,
        bsdf_folder=bsdfs
    ):
        return [
            {
                'from': DaylightContribution()._outputs.result_file,
                'to': 'total_sky.ill'
            }
        ]

    @task(
        template=AddRemoveSkyMatrix,
        needs=[direct_sun, total_sky, direct_sky]
    )
    def output_matrix_math(
        self,
        name=grid_name,
        direct_sky_matrix=direct_sky._outputs.result_file,
        total_sky_matrix=total_sky._outputs.result_file,
        sunlight_matrix=direct_sun._outputs.result_file,
    ):
        return [
            {
                'from': AddRemoveSkyMatrix()._outputs.results_file,
                'to': '../final/total/{{self.name}}.ill'
            }
        ]
