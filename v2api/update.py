import os
import sys
from socrata.authorization import Authorization
from socrata import Socrata

auth = Authorization(
    'oakland-staging.demo.socrata.com',
    os.environ['SOCRATA_USERNAME'],
    os.environ['SOCRATA_PASSWORD']
)

socrata = Socrata(auth)
view = socrata.views.lookup('fx5i-jz4n')

with open('output/contribs_socrata.csv', 'rb') as my_file:
    (revision, job) = socrata.using_config(
        'contribs_socrata[88]_08-08-2022_67df', view).csv(my_file)
    # These next 2 lines are optional - once the job is started from the previous line, the
    # script can exit; these next lines just block until the job completes
    job = job.wait_for_finish(progress=lambda job: print(
        'Job progress:', job.attributes['status']))
    sys.exit(0 if job.attributes['status'] == 'successful' else 1)
