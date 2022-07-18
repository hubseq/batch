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

MOCK_RESPONSE = {'ResponseMetadata': {'RequestId': '6dd9979b-c64c-4629-8089-2f4525c184ac', 'HTTPStatusCode': 200, 'HTTPHeaders': {'date': 'Mon, 18 Jul 2022 20:34:42 GMT', 'content-type': 'application/json', 'content-length': '8095', 'connection': 'keep-alive', 'x-amzn-requestid': '6dd9979b-c64c-4629-8089-2f4525c184ac', 'access-control-allow-origin': '*', 'x-amz-apigw-id': 'VeuLcG17PHcFW2A=', 'access-control-expose-headers': 'X-amzn-errortype,X-amzn-requestid,X-amzn-errormessage,X-amzn-trace-id,X-amz-apigw-id,date', 'x-amzn-trace-id': 'Root=1-62d5c3e2-445255b659e6b2980608a2ee'}, 'RetryAttempts': 0}, 'jobs': [{'jobArn': 'arn:aws:batch:us-west-2:700080227344:job/c6cfb43f-ad41-4488-ad42-0f3cd751e790', 'jobName': 'job_goqc_e7a15d85-4f79-4581-acbb-bd63772cb69e', 'jobId': 'c6cfb43f-ad41-4488-ad42-0f3cd751e790', 'jobQueue': 'arn:aws:batch:us-west-2:700080227344:job-queue/batch_scratch_queue_public', 'status': 'SUCCEEDED', 'attempts': [{'container': {'containerInstanceArn': 'arn:aws:ecs:us-west-2:700080227344:container-instance/batch_compute_tiny_public_Batch_477b7626-6a6d-3c61-a669-987b29598f0a/bb89e659f39d4887a2bf181dc2823bb7', 'taskArn': 'arn:aws:ecs:us-west-2:700080227344:task/batch_compute_tiny_public_Batch_477b7626-6a6d-3c61-a669-987b29598f0a/822fefa67acd4771befd18d2de289da2', 'exitCode': 0, 'logStreamName': 'jdef_goqc_e7a15d85-4f79-4581-acbb-bd63772cb69e/default/822fefa67acd4771befd18d2de289da2', 'networkInterfaces': []}, 'startedAt': 1658125926379, 'stoppedAt': 1658125928496, 'statusReason': 'Essential container in task exited'}], 'statusReason': 'Essential container in task exited', 'createdAt': 1658125650599, 'retryStrategy': {'attempts': 3, 'evaluateOnExit': []}, 'startedAt': 1658125926379, 'stoppedAt': 1658125928496, 'dependsOn': [], 'jobDefinition': 'arn:aws:batch:us-west-2:700080227344:job-definition/jdef_goqc_e7a15d85-4f79-4581-acbb-bd63772cb69e:1', 'parameters': {}, 'container': {'image': '700080227344.dkr.ecr.us-west-2.amazonaws.com/goqc:00.00.00', 'vcpus': 2, 'memory': 6000, 'command': ['--module_name', 'goqc', '--run_arguments', 's3://hubseq-data/modules/goqc/io/goqc.e7a15d85-4f79-4581-acbb-bd63772cb69e.io.json', '--working_dir', '/home'], 'jobRoleArn': 'arn:aws:iam::700080227344:role/ecs_batch_service_role', 'volumes': [], 'environment': [], 'mountPoints': [], 'ulimits': [], 'exitCode': 0, 'containerInstanceArn': 'arn:aws:ecs:us-west-2:700080227344:container-instance/batch_compute_tiny_public_Batch_477b7626-6a6d-3c61-a669-987b29598f0a/bb89e659f39d4887a2bf181dc2823bb7', 'taskArn': 'arn:aws:ecs:us-west-2:700080227344:task/batch_compute_tiny_public_Batch_477b7626-6a6d-3c61-a669-987b29598f0a/822fefa67acd4771befd18d2de289da2', 'logStreamName': 'jdef_goqc_e7a15d85-4f79-4581-acbb-bd63772cb69e/default/822fefa67acd4771befd18d2de289da2', 'networkInterfaces': [], 'resourceRequirements': [], 'secrets': []}, 'tags': {}, 'platformCapabilities': []}, {'jobArn': 'arn:aws:batch:us-west-2:700080227344:job/76065d62-0454-4a98-ba13-538112b7eed7', 'jobName': 'job_david_go_67f205b4-1a2a-44ae-83e1-82fc1a9c1630', 'jobId': '76065d62-0454-4a98-ba13-538112b7eed7', 'jobQueue': 'arn:aws:batch:us-west-2:700080227344:job-queue/batch_scratch_queue_public', 'status': 'SUCCEEDED', 'attempts': [{'container': {'containerInstanceArn': 'arn:aws:ecs:us-west-2:700080227344:container-instance/batch_compute_tiny_public_Batch_477b7626-6a6d-3c61-a669-987b29598f0a/ed3179c7fdec4bd782e6a472b84a3790', 'taskArn': 'arn:aws:ecs:us-west-2:700080227344:task/batch_compute_tiny_public_Batch_477b7626-6a6d-3c61-a669-987b29598f0a/7adb27dab25642e985c0fe29e750c46d', 'exitCode': 0, 'logStreamName': 'jdef_david_go_67f205b4-1a2a-44ae-83e1-82fc1a9c1630/default/7adb27dab25642e985c0fe29e750c46d', 'networkInterfaces': []}, 'startedAt': 1658002865068, 'stoppedAt': 1658002869658, 'statusReason': 'Essential container in task exited'}], 'statusReason': 'Essential container in task exited', 'createdAt': 1658002504378, 'retryStrategy': {'attempts': 3, 'evaluateOnExit': []}, 'startedAt': 1658002865068, 'stoppedAt': 1658002869658, 'dependsOn': [{'jobId': '89c72f06-8940-438a-b925-12ee9764988a'}], 'jobDefinition': 'arn:aws:batch:us-west-2:700080227344:job-definition/jdef_david_go_67f205b4-1a2a-44ae-83e1-82fc1a9c1630:1', 'parameters': {}, 'container': {'image': '700080227344.dkr.ecr.us-west-2.amazonaws.com/david_go:00.00.01', 'vcpus': 2, 'memory': 6000, 'command': ['--module_name', 'david_go', '--run_arguments', 's3://hubseq-data/modules/david_go/io/david_go.67f205b4-1a2a-44ae-83e1-82fc1a9c1630.io.json', '--working_dir', '/home'], 'jobRoleArn': 'arn:aws:iam::700080227344:role/ecs_batch_service_role', 'volumes': [], 'environment': [], 'mountPoints': [], 'ulimits': [], 'exitCode': 0, 'containerInstanceArn': 'arn:aws:ecs:us-west-2:700080227344:container-instance/batch_compute_tiny_public_Batch_477b7626-6a6d-3c61-a669-987b29598f0a/ed3179c7fdec4bd782e6a472b84a3790', 'taskArn': 'arn:aws:ecs:us-west-2:700080227344:task/batch_compute_tiny_public_Batch_477b7626-6a6d-3c61-a669-987b29598f0a/7adb27dab25642e985c0fe29e750c46d', 'logStreamName': 'jdef_david_go_67f205b4-1a2a-44ae-83e1-82fc1a9c1630/default/7adb27dab25642e985c0fe29e750c46d', 'networkInterfaces': [], 'resourceRequirements': [], 'secrets': []}, 'tags': {}, 'platformCapabilities': []}, {'jobArn': 'arn:aws:batch:us-west-2:700080227344:job/08763c3d-d330-4725-aa8d-6b7c5cf956a2', 'jobName': 'job_deqc_c470aea2-f776-40e4-b3ff-32ac6d4cfc80', 'jobId': '08763c3d-d330-4725-aa8d-6b7c5cf956a2', 'jobQueue': 'arn:aws:batch:us-west-2:700080227344:job-queue/batch_scratch_queue_public', 'status': 'SUCCEEDED', 'attempts': [{'container': {'containerInstanceArn': 'arn:aws:ecs:us-west-2:700080227344:container-instance/batch_compute_tiny_public_Batch_477b7626-6a6d-3c61-a669-987b29598f0a/ed3179c7fdec4bd782e6a472b84a3790', 'taskArn': 'arn:aws:ecs:us-west-2:700080227344:task/batch_compute_tiny_public_Batch_477b7626-6a6d-3c61-a669-987b29598f0a/4ef6c815375142908d66c4fa7abe4e5f', 'exitCode': 0, 'logStreamName': 'jdef_deqc_c470aea2-f776-40e4-b3ff-32ac6d4cfc80/default/4ef6c815375142908d66c4fa7abe4e5f', 'networkInterfaces': []}, 'startedAt': 1658002769812, 'stoppedAt': 1658002771955, 'statusReason': 'Essential container in task exited'}], 'statusReason': 'Essential container in task exited', 'createdAt': 1658002503721, 'retryStrategy': {'attempts': 3, 'evaluateOnExit': []}, 'startedAt': 1658002769812, 'stoppedAt': 1658002771955, 'dependsOn': [{'jobId': '89c72f06-8940-438a-b925-12ee9764988a'}], 'jobDefinition': 'arn:aws:batch:us-west-2:700080227344:job-definition/jdef_deqc_c470aea2-f776-40e4-b3ff-32ac6d4cfc80:1', 'parameters': {}, 'container': {'image': '700080227344.dkr.ecr.us-west-2.amazonaws.com/deqc:00.00.00', 'vcpus': 2, 'memory': 6000, 'command': ['--module_name', 'deqc', '--run_arguments', 's3://hubseq-data/modules/deqc/io/deqc.c470aea2-f776-40e4-b3ff-32ac6d4cfc80.io.json', '--working_dir', '/home'], 'jobRoleArn': 'arn:aws:iam::700080227344:role/ecs_batch_service_role', 'volumes': [], 'environment': [], 'mountPoints': [], 'ulimits': [], 'exitCode': 0, 'containerInstanceArn': 'arn:aws:ecs:us-west-2:700080227344:container-instance/batch_compute_tiny_public_Batch_477b7626-6a6d-3c61-a669-987b29598f0a/ed3179c7fdec4bd782e6a472b84a3790', 'taskArn': 'arn:aws:ecs:us-west-2:700080227344:task/batch_compute_tiny_public_Batch_477b7626-6a6d-3c61-a669-987b29598f0a/4ef6c815375142908d66c4fa7abe4e5f', 'logStreamName': 'jdef_deqc_c470aea2-f776-40e4-b3ff-32ac6d4cfc80/default/4ef6c815375142908d66c4fa7abe4e5f', 'networkInterfaces': [], 'resourceRequirements': [], 'secrets': []}, 'tags': {}, 'platformCapabilities': []}]}

def get_jobstatus( args_json ):
    # get list of job IDs
    jobs_list = args_json['jobs'].split(',')
    job_info_json = {}

    # mock runs - just return dummy JSON
    if 'mock' in args_json and args_json['mock'] == True:
        mock_json = MOCK_RESPONSE
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
    
