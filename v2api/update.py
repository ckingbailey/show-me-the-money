import argparse
import os
import sys
from socrata.authorization import Authorization
from socrata import Socrata

auth = Authorization(
    'data.oaklandca.gov',
    os.environ['OAKDATA_KEY'],
    os.environ['OAKDATA_SECRET']
)
socrata = Socrata(auth)

def update_dataset(dataset_id: str, update_config_id: str, data_file: str):
    """ Call Socrata API to update dataset with csv file """
    view = socrata.views.lookup(dataset_id)

    with open(data_file, 'rb') as f:
        revision, job = socrata.using_config(
            update_config_id, view).csv(f)

        # These next 2 lines are optional - once the job is started from the previous line, the
        # script can exit; these next lines just block until the job completes
        job = job.wait_for_finish(progress=lambda job: print(
            'Job progress:', job.attributes['status']))

        print(f'Dataset {dataset_id} update {job.attributes["status"]}')

def main():
    """ Update all datasets
    """
    datasets = [
        {
            'id': 'iwe7-af4m',
            'update_config_id': 'contribs_socrata_08-29-2022_1b01',
            'file': 'output/contribs_socrata.csv'
        },
        {
            'id': 'yjtu-3cj6',
            'update_config_id': 'expends_socrata_08-29-2022_7de1',
            'file': 'output/expends_socrata.csv'
        }
    ]

    for dataset in datasets:
        update_dataset(dataset['id'], dataset['update_config_id'], dataset['file'])

if __name__ == '__main__':
    main()
