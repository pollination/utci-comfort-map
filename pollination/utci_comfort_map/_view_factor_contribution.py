from pollination_dsl.dag import Inputs, DAG, task
from dataclasses import dataclass
from typing import Dict, List

# pollination plugins and recipes
from pollination.honeybee_radiance_postprocess.viewfactor import SphericalViewFactorContribution


@dataclass
class SphericalViewFactor(DAG):
    """Prepare folder for two phase daylight coefficient."""

    # inputs
    radiance_parameters = Inputs.str(
        description='Radiance parameters for ray tracing.',
        default='-ab 2 -ad 5000 -lw 2e-05',
    )

    scene_file = Inputs.file(
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

    modifiers = Inputs.file(
        description='A file with surface modifiers.'
    )

    @task(
        template=SphericalViewFactorContribution
    )
    def compute_spherical_view_factors(
        self,
        name=grid_name,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -ab 1 -c 1 -faf',
        modifiers=modifiers,
        sensor_grid=sensor_grid,
        scene_file=scene_file
    ) -> List[Dict]:
        return [
            {
                'from': SphericalViewFactorContribution()._outputs.view_factor_file,
                'to': 'initial_results/{{self.name}}.npy'
            }
        ]
