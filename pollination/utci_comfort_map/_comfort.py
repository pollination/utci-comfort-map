from pollination_dsl.dag import Inputs, GroupedDAG, task
from dataclasses import dataclass
from typing import Dict, List

from pollination.ladybug_comfort.epw import AirSpeedJson
from pollination.ladybug_comfort.map import ShortwaveMrtMap, LongwaveMrtMap, AirMap, Tcp
from pollination.ladybug_comfort.mtx import UtciMtx


@dataclass
class ComfortMappingEntryPoint(GroupedDAG):
    """Entry point for Comfort calculations."""

    # inputs
    epw = Inputs.file(
        description='Weather file used for the comfort map.',
        extensions=['epw']
    )

    result_sql = Inputs.file(
        description='A SQLite file that was generated by EnergyPlus and contains '
        'hourly or sub-hourly thermal comfort results.',
        extensions=['sql', 'db', 'sqlite'], optional=True
    )

    grid_name = Inputs.str(
        description='Sensor grid file name (used to name the final result files).'
    )

    enclosure_info = Inputs.file(
        description='A JSON file containing information about the radiant '
        'enclosure that sensor points belong to.',
        extensions=['json']
    )

    view_factors = Inputs.file(
        description='A CSV of spherical view factors to the surfaces in the result-sql.',
        extensions=['npy']
    )

    modifiers = Inputs.file(
        description='Path to a modifiers file that aligns with the view-factors.',
        extensions=['mod', 'txt']
    )

    indirect_irradiance = Inputs.file(
        description='An .ill containing the indirect irradiance for each sensor.',
        extensions=['ill', 'irr']
    )

    direct_irradiance = Inputs.file(
        description='An .ill containing direct irradiance for each sensor.',
        extensions=['ill', 'irr']
    )

    ref_irradiance = Inputs.file(
        description='An .ill containing ground-reflected irradiance for each '
        'sensor.', extensions=['ill', 'irr']
    )

    sun_up_hours = Inputs.file(
        description='A sun-up-hours.txt file output by Radiance and aligns with the '
        'input irradiance files.'
    )

    occ_schedules = Inputs.file(
        description='A JSON file containing occupancy schedules derived from '
        'the input model.'
    )

    schedule = Inputs.file(
        description='A CSV file containing a single number for meteorological wind '
        'speed in m/s or several rows of wind speeds that align with the length of the '
        'run period. This will be used for all outdoor comfort evaluation.',
        optional=True
    )

    transmittance_contribs = Inputs.folder(
        description='An optional folder containing a transmittance schedule JSON '
        'and sub-folders of irradiance results that exclude the shade from the '
        'calculation. There should be one sub-folder per window groups and each '
        'one should contain three .ill files named direct.ill, indirect.ill and '
        'reflected.ill. If specified, these will be added to the irradiance inputs '
        'before computing shortwave MRT deltas.', optional=True
    )

    trans_schedules = Inputs.file(
        description='A schedule JSON that contains fractional schedule values '
        'for each shade transmittance schedule in the model.'
    )

    run_period = Inputs.str(
        description='An AnalysisPeriod string to set the start and end dates of '
        'the simulation (eg. "6/21 to 9/21 between 0 and 23 @1"). If None, '
        'the simulation will be annual.', default=''
    )

    wind_speed = Inputs.file(
        description='A CSV with numbers that align with the input run period. '
        'This will be used for all outdoor comfort evaluation. If None, '
        'the EPW wind speed will be used for all outdoor sensors.', optional=True
    )

    air_speed_mtx = Inputs.file(
        description='A CSV file with with a matrix of air speed values in m/s. '
        'Note that these values are not meteorological and should be AT HUMAN '
        'SUBJECT LEVEL. If specified, this overrides the wind-speed input.',
        optional=True
    )

    solarcal_parameters = Inputs.str(
        description='A SolarCalParameter string to customize the assumptions of '
        'the SolarCal model.', default='--posture standing --sharp 135 '
        '--absorptivity 0.7 --emissivity 0.95'
    )

    comfort_parameters = Inputs.str(
        description='An UTCIParameter string to customize the assumptions of '
        'the UTCI comfort model.', default='--cold 9 --heat 26'
    )

    @task(template=LongwaveMrtMap)
    def create_longwave_mrt_map(
        self,
        result_sql=result_sql,
        view_factors=view_factors,
        modifiers=modifiers,
        enclosure_info=enclosure_info,
        epw=epw,
        run_period=run_period,
        name=grid_name,
        output_format='binary'
    ) -> List[Dict]:
        return [
            {
                'from': LongwaveMrtMap()._outputs.longwave_mrt_map,
                'to': 'conditions/longwave_mrt/{{self.name}}.csv'
            }
        ]

    @task(template=ShortwaveMrtMap)
    def create_shortwave_mrt_map(
        self,
        epw=epw,
        indirect_irradiance=indirect_irradiance,
        direct_irradiance=direct_irradiance,
        ref_irradiance=ref_irradiance,
        sun_up_hours=sun_up_hours,
        transmittance_contribs=transmittance_contribs,
        trans_schedules=trans_schedules,
        solarcal_par=solarcal_parameters,
        run_period=run_period,
        name=grid_name,
        output_format='binary'
    ) -> List[Dict]:
        return [
            {
                'from': ShortwaveMrtMap()._outputs.shortwave_mrt_map,
                'to': 'conditions/shortwave_mrt/{{self.name}}.csv'
            }
        ]

    @task(template=AirMap)
    def create_air_temperature_map(
        self,
        result_sql=result_sql,
        enclosure_info=enclosure_info,
        epw=epw,
        run_period=run_period,
        metric='air-temperature',
        name=grid_name,
        output_format='binary'
    ) -> List[Dict]:
        return [
            {
                'from': AirMap()._outputs.air_map,
                'to': 'conditions/air_temperature/{{self.name}}.csv'
            }
        ]

    @task(template=AirMap)
    def create_rel_humidity_map(
        self,
        result_sql=result_sql,
        enclosure_info=enclosure_info,
        epw=epw,
        run_period=run_period,
        metric='relative-humidity',
        name=grid_name,
        output_format='binary'
    ) -> List[Dict]:
        return [
            {
                'from': AirMap()._outputs.air_map,
                'to': 'conditions/rel_humidity/{{self.name}}.csv'
            }
        ]

    @task(template=AirSpeedJson)
    def create_air_speed_json(
        self, epw=epw, enclosure_info=enclosure_info, multiply_by=1.0,
        outdoor_air_speed=wind_speed,
        run_period=run_period, name=grid_name
    ) -> List[Dict]:
        return [
            {
                'from': AirSpeedJson()._outputs.air_speeds,
                'to': 'conditions/air_speed/{{self.name}}.json'
            }
        ]

    @task(
        template=UtciMtx,
        needs=[
            create_longwave_mrt_map, create_shortwave_mrt_map,
            create_air_temperature_map, create_rel_humidity_map, create_air_speed_json
        ]
    )
    def process_utci_matrix(
        self,
        air_temperature_mtx=create_air_temperature_map._outputs.air_map,
        rel_humidity_mtx=create_rel_humidity_map._outputs.air_map,
        rad_temperature_mtx=create_longwave_mrt_map._outputs.longwave_mrt_map,
        rad_delta_mtx=create_shortwave_mrt_map._outputs.shortwave_mrt_map,
        wind_speed_json=create_air_speed_json._outputs.air_speeds,
        air_speed_mtx=air_speed_mtx,
        comfort_par=comfort_parameters,
        name=grid_name,
        output_format='binary'
    ) -> List[Dict]:
        return [
            {
                'from': UtciMtx()._outputs.temperature_map,
                'to': 'results/temperature/{{self.name}}.csv'
            },
            {
                'from': UtciMtx()._outputs.condition_map,
                'to': 'results/condition/{{self.name}}.csv'
            },
            {
                'from': UtciMtx()._outputs.category_map,
                'to': 'results/condition_intensity/{{self.name}}.csv'
            }
        ]

    @task(
        template=Tcp,
        needs=[process_utci_matrix]
    )
    def compute_tcp(
        self,
        condition_csv=process_utci_matrix._outputs.condition_map,
        enclosure_info=enclosure_info,
        occ_schedule_json=occ_schedules,
        schedule=schedule,
        name=grid_name
    ) -> List[Dict]:
        return [
            {'from': Tcp()._outputs.tcp, 'to': 'metrics/TCP/{{self.name}}.csv'},
            {'from': Tcp()._outputs.hsp, 'to': 'metrics/HSP/{{self.name}}.csv'},
            {'from': Tcp()._outputs.csp, 'to': 'metrics/CSP/{{self.name}}.csv'}
        ]
