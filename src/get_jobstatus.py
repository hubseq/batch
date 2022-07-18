#
# get_jobstatus
#
# Get status of a list of jobs
#
# 'jobs': string list of jobs (comma separated)
#
import os, sys, uuid, json, boto3
sys.path.append('global_utils/src/')
import module_utils
import file_utils
from argparse import ArgumentParser
from datetime import datetime

SCRIPT_DIR = str(os.path.dirname(os.path.realpath(__file__)))
BATCH_SETTINGS_FILE = os.path.join(SCRIPT_DIR, 'batch.settings.json')

def get_jobstatus( args_json ):
    # get list of job IDs
    jobs_list = args_json['jobs'].split(',')
    job_info_json = {}

    # mock runs - just return dummy JSON
    if 'mock' in args_json and args_json['mock'] == True:
        mock_json = {'mock': 'mock get_jobstatus()'}
        return mock_json
    
    if not module_utils.isDryRun( args_json ):
        # use boto3 to fetch job info based on job IDs
        batch_defaults_json = file_utils.loadJSON(BATCH_SETTINGS_FILE)

        # initialize Batch boto3 client access
        print('\nSetting up boto3 client in {}...'.format(batch_defaults_json['aws_region']))
        client = boto3.client('batch', region_name=batch_defaults_json['aws_region'])

        job_info_json = client.describe_jobs( jobs = jobs_list )
    else:
        print('DRY RUN. ')
    return job_info_json

if __name__ == '__main__':
    def error(self, message):
        sys.stderr.write('Usage: %s\n' % message)        
        self.print_help()
        sys.exit(2)
    # print('USAGE: $ python get_jobstatus.py --jobs <JOBS> --dryrun')
    argparser = ArgumentParser()
    file_path_group = argparser.add_argument_group(title='Get job status arguments')
    file_path_group.add_argument('--jobs', '-j', help='comma-separated list of job IDs', required=True)
    file_path_group.add_argument('--dryrun', help='dry run only', required=False, action='store_true')
    file_path_group.add_argument('--mock', help='mock run only', required=False, action='store_true')
    file_path_group.add_argument('--scratchdir', help='scratch directory for storing temp files', required=False, default='/home/')
    get_jobstatus_args = argparser.parse_args()
    jobinfo_json = get_jobstatus( vars(get_jobstatus_args) )
    print('JOB ID INFO JSON OUT')
    print(str(jobinfo_json))
    
