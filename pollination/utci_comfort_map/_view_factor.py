from pollination_dsl.dag import Inputs, DAG, task
from dataclasses import dataclass
from typing import Dict, List

# pollination plugins and recipes
from pollination.honeybee_radiance.modifier import SplitModifiers
from pollination.honeybee_radiance_postprocess.merge import MergeFiles

from ._view_factor_contribution import SphericalViewFactor


@dataclass
class SphericalViewFactorEntryPoint(DAG):
    """Prepare folder for two phase daylight coefficient."""

    # inputs
    radiance_parameters = Inputs.str(
        description='Radiance parameters for ray tracing.',
        default='-ab 2 -ad 5000 -lw 2e-05',
    )

    octree_file_view_factor = Inputs.file(
        description='A Radiance octree file with surface view factor modifiers.',
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

    view_factor_modifiers = Inputs.file(
        description='A file with surface modifiers.'
    )

    @task(template=SplitModifiers)
    def split_modifiers(
        self,
        modifier_file=view_factor_modifiers,
        grid_file=sensor_grid,
        max_value=200000000,
        sensor_multiplier=6
    ):
        return [
            {
                'from': SplitModifiers()._outputs.output_folder,
                'to': 'split_modifiers'
            },
            {
                'from': SplitModifiers()._outputs.modifiers
            }
        ]

    @task(
        template=SphericalViewFactor,
        needs=[split_modifiers],
        loop=split_modifiers._outputs.modifiers,
        sub_paths={
            'modifiers': '{{item.identifier}}.mod'
        }
    )
    def calculate_spherical_view_factors(
        self,
        grid_name='{{item.identifier}}',
        radiance_parameters=radiance_parameters,
        modifiers=split_modifiers._outputs.output_folder,
        sensor_grid=sensor_grid,
        scene_file=octree_file_view_factor
    ):
        pass

    @task(
        template=MergeFiles,
        needs=[calculate_spherical_view_factors, split_modifiers],
        sub_paths={
            'dist_info': '_redist_info.json'
        }
    )
    def restructure_view_factor(
        self, name=grid_name, input_folder='initial_results',
        extension='npy', dist_info=split_modifiers._outputs.output_folder,
        merge_axis=1
    ) -> List[Dict]:
        return [
            {
                'from': MergeFiles()._outputs.output_file,
                'to': '../../longwave/view_factors/{{self.name}}.npy'
            }
        ]
