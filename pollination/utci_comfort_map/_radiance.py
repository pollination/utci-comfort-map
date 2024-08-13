from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from dataclasses import dataclass

from pollination.honeybee_radiance.grid import MirrorGrid, RadiantEnclosureInfo
from pollination.honeybee_radiance.contrib import DaylightContribution
from pollination.honeybee_radiance.coefficient import DaylightCoefficient
from pollination.honeybee_radiance.sky import SubtractSkyMatrix


@dataclass
class RadianceMappingEntryPoint(GroupedDAG):
    """Entry point for Radiance calculations for comfort mapping."""

    # inputs
    radiance_parameters = Inputs.str(
        description='Radiance parameters for ray tracing.',
        default='-ab 2 -ad 5000 -lw 2e-05',
    )

    model = Inputs.file(
        description='A Honeybee model in HBJSON file format.',
        extensions=['json', 'hbjson']
    )

    octree_file_with_suns = Inputs.file(
        description='A Radiance octree file with sun modifiers.',
        extensions=['oct']
    )

    octree_file = Inputs.file(
        description='A Radiance octree file with a sky dome.',
        extensions=['oct']
    )

    grid_name = Inputs.str(
        description='Sensor grid file name (used to name the final result files).'
    )

    sensor_grid = Inputs.file(
        description='Sensor grid file.',
        extensions=['pts']
    )

    sensor_count = Inputs.int(
        description='Number of sensors in the input sensor grid.'
    )

    sky_dome = Inputs.file(
        description='Path to sky dome file.'
    )

    sky_matrix = Inputs.file(
        description='Path to total sky matrix file.'
    )

    sky_matrix_direct = Inputs.file(
        description='Path to direct skymtx file (gendaymtx -d).'
    )

    sun_modifiers = Inputs.file(
        description='A file with sun modifiers.'
    )

    @task(template=RadiantEnclosureInfo)
    def get_enclosure_info(self, model=model, input_grid=sensor_grid, name=grid_name):
        return [
            {
                'from': RadiantEnclosureInfo()._outputs.enclosure_file,
                'to': 'enclosures/{{self.name}}.json'
            }
        ]

    @task(template=MirrorGrid)
    def mirror_the_grid(self, input_grid=sensor_grid, name=grid_name, vector='0 0 1'):
        return [
            {
                'from': MirrorGrid()._outputs.base_file,
                'to': 'shortwave/grids/{{self.name}}.pts'
            },
            {
                'from': MirrorGrid()._outputs.mirrored_file,
                'to': 'shortwave/grids/{{self.name}}_ref.pts'
            }
        ]

    @task(template=DaylightContribution, needs=[mirror_the_grid])
    def direct_sun(
        self,
        name=grid_name,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -ab 0 -dc 1.0 -dt 0.0 -dj 0.0 -dr 0',
        sensor_count=sensor_count,
        modifiers=sun_modifiers,
        sensor_grid=mirror_the_grid._outputs.base_file,
        conversion='0.265 0.670 0.065',
        output_format='f',  # make it ascii so we expose the file as a separate output
        header='keep',  # remove header to make it process-able later
        scene_file=octree_file_with_suns
    ):
        return [
            {
                'from': DaylightContribution()._outputs.result_file,
                'to': 'shortwave/results/direct/{{self.name}}.ill'
            }
        ]

    @task(template=DaylightCoefficient, needs=[mirror_the_grid])
    def direct_sky(
        self,
        name=grid_name,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -ab 1 -c 1 -faf',
        sensor_count=sensor_count,
        sky_matrix=sky_matrix_direct,
        sky_dome=sky_dome,
        sensor_grid=mirror_the_grid._outputs.base_file,
        conversion='0.265 0.670 0.065',  # divide by 179
        scene_file=octree_file
    ):
        return [
            {
                'from': DaylightCoefficient()._outputs.result_file,
                'to': 'shortwave/initial_results/direct_sky/{{self.name}}.ill'
            }
        ]

    @task(template=DaylightCoefficient, needs=[mirror_the_grid])
    def total_sky(
        self,
        name=grid_name,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -c 1 -faf',
        sensor_count=sensor_count,
        sky_matrix=sky_matrix,
        sky_dome=sky_dome,
        sensor_grid=mirror_the_grid._outputs.base_file,
        conversion='0.265 0.670 0.065',  # divide by 179
        scene_file=octree_file
    ):
        return [
            {
                'from': DaylightCoefficient()._outputs.result_file,
                'to': 'shortwave/initial_results/total_sky/{{self.name}}.ill'
            }
        ]

    @task(template=SubtractSkyMatrix, needs=[total_sky, direct_sky])
    def output_matrix_math(
        self,
        name=grid_name,
        total_sky_matrix=total_sky._outputs.result_file,
        direct_sky_matrix=direct_sky._outputs.result_file,
        output_format='f',
        header='keep'
    ):
        return [
            {
                'from': SubtractSkyMatrix()._outputs.results_file,
                'to': 'shortwave/results/indirect/{{self.name}}.ill'
            }
        ]

    @task(template=DaylightCoefficient, needs=[mirror_the_grid])
    def ground_reflected_sky(
        self,
        name=grid_name,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -c 1',
        sensor_count=sensor_count,
        sky_matrix=sky_matrix,
        sky_dome=sky_dome,
        sensor_grid=mirror_the_grid._outputs.mirrored_file,
        conversion='0.265 0.670 0.065',  # divide by 179
        output_format='f',  # make it ascii so we expose the file as a separate output
        header='keep',  # remove header to make it process-able later
        scene_file=octree_file
    ):
        return [
            {
                'from': DaylightCoefficient()._outputs.result_file,
                'to': 'shortwave/results/reflected/{{self.name}}.ill'
            }
        ]


    enclosures = Outputs.folder(source='enclosures')

    shortwave_results = Outputs.folder(source='shortwave/results')

    shortwave_grids = Outputs.folder(source='shortwave/grids')
