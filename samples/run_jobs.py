"""Run sample runs on Pollination."""
from pathlib import Path
import json
import datetime
import time
import sys
import os
from requests.exceptions import HTTPError

from pollination_io.api.client import ApiClient
from pollination_io.interactors import NewJob, Recipe
from queenbee.job.job import JobStatusEnum


# get environment variables
api_key = os.environ['QB_POLLINATION_TOKEN']
recipe_tag = os.environ['TAG']
host = os.environ['HOST']

owner = 'ladybug-tools'
project = 'utci-comfort-map'
recipe_name = 'utci-comfort-map'

api_client = ApiClient(host, api_key)
recipe = Recipe(owner, recipe_name, recipe_tag, client=api_client)
recipe.add_to_project(f'{owner}/{project}')

# load recipe inputs for each sample run
samples_path = Path(__file__).parent.resolve().joinpath('sample_runs.json')
with open(samples_path, encoding='utf-8') as samples_json:
    sample_runs = json.load(samples_json)

# create a new job
datetime_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
name = f'Samples (Scheduled by GitHub workflow: {datetime_now})'
new_study = NewJob(owner, project, recipe, name=name, client=api_client)

# get all unique artifacts
artifacts = set()
for sample_run in sample_runs:
    for recipe_input, value in sample_run['artifacts'].items():
        input_path = Path(__file__).parent.resolve().joinpath(value)
        assert input_path.exists(), f'{input_path} does not exist.'
        artifacts.add(value)

# upload unique artifacts
artifacts_path = {}
for artifact in artifacts:
    input_path = Path(__file__).parent.resolve().joinpath(artifact)
    artifact_path = new_study.upload_artifact(input_path)
    artifacts_path[artifact] = artifact_path

# get recipe inputs for each run and upload artifact
study_inputs = []
for sample_run in sample_runs:
    inputs = dict(sample_run['inputs'])
    for recipe_input, value in sample_run['artifacts'].items():
        inputs[recipe_input] = artifacts_path[value]
    study_inputs.append(inputs)

# add the inputs to the study
new_study.arguments = study_inputs

# create the study
running_study = new_study.create()
if host == 'https://api.staging.pollination.cloud':
    pollination_url = 'https://app.staging.pollination.cloud'
else:
    pollination_url = 'https://app.pollination.cloud'
job_url = f'{pollination_url}/{running_study.owner}/projects/{running_study.project}/jobs/{running_study.id}'
print(job_url)

# wait for 5 seconds
time.sleep(5)

# check status of study
status = running_study.status.status
http_errors = 0
while True:
    status_info = running_study.status
    print('\t# ------------------ #')
    print(f'\t# pending runs: {status_info.runs_pending}')
    print(f'\t# running runs: {status_info.runs_running}')
    print(f'\t# failed runs: {status_info.runs_failed}')
    print(f'\t# completed runs: {status_info.runs_completed}')
    if status in [
        JobStatusEnum.pre_processing, JobStatusEnum.running, JobStatusEnum.created,
        JobStatusEnum.unknown
    ]:
        time.sleep(15)
        try:
            running_study.refresh()
        except HTTPError as e:
            status_code = e.response.status_code
            print(str(e))
            if status_code == 500:
                http_errors += 1
                if http_errors > 3:
                    # failed for than 3 times with no success
                    raise HTTPError(e)
                # wait for additional 10 seconds
                time.sleep(10)
        else:
            http_errors = 0
            status = status_info.status
    else:
        # study is finished
        time.sleep(2)
        break

# return exit status
if status_info.runs_failed != 0:
    sys.exit(1)
else:
    sys.exit(0)
