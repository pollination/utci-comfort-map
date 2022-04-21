from pollination_dsl.dag import Inputs, DAG, task
from dataclasses import dataclass
from typing import Dict, List

from pollination.path.read import ReadJSONList

from ._shdcontrib import ShadeContribEntryPoint


@dataclass
class DynamicShadeContribEntryPoint(DAG):
    """Entry point for computing the contributions from dynamic windows."""

    # inputs
    radiance_parameters = Inputs.str(
        description='Radiance parameters for ray tracing.',
        default='-ab 2 -ad 5000 -lw 2e-05',
    )

    octree_file = Inputs.file(
        description='A Radiance octree file with a completely transparent version '
        'of the dynamic shade group.', extensions=['oct']
    )

    octree_file_with_suns = Inputs.file(
        description='A Radiance octree file with sun modifiers.',
        extensions=['oct']
    )

    group_name = Inputs.str(
        description='Name for the dynamic aperture group being simulated.'
    )

    sensor_grid_folder = Inputs.folder(
        description='A folder containing all of the split sensor grids in the model.'
    )

    sensor_grids = Inputs.file(
        description='A JSON file with information about sensor grids to loop over.'
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

    sun_up_hours = Inputs.file(
        description='A sun-up-hours.txt file output by Radiance and aligns with the '
        'input irradiance files.'
    )

    @task(template=ReadJSONList)
    def read_grids(self, src=sensor_grids) -> List[Dict]:
        return [
            {
                'from': ReadJSONList()._outputs.data,
                'description': 'Sensor grids information.'
            }
        ]

    @task(
        template=ShadeContribEntryPoint,
        needs=[read_grids],
        loop=read_grids._outputs.data,
        sub_folder='shortwave',
        sub_paths={
            'sensor_grid': '{{item.full_id}}.pts',
            'ref_sensor_grid': '{{item.full_id}}_ref.pts',
        }
    )
    def run_radiance_window_contrib(
        self,
        radiance_parameters=radiance_parameters,
        octree_file=octree_file,
        octree_file_with_suns=octree_file_with_suns,
        group_name=group_name,
        grid_name='{{item.full_id}}',
        sensor_grid=sensor_grid_folder,
        ref_sensor_grid=sensor_grid_folder,
        sensor_count='{{item.count}}',
        sky_dome=sky_dome,
        sky_matrix=sky_matrix,
        sky_matrix_direct=sky_matrix,
        sun_modifiers=sun_modifiers,
        sun_up_hours=sun_up_hours
    ) -> List[Dict]:
        pass
