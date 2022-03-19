#
# run_batchjob
#
# Takes a string of command-line arguments and submits a batch job.
#
# Possible arguments:
#
# -dependentid=<ID>
# -sampleid=MYSAMPLE
# -input=<INPUT_FILE>
# -output=<OUT_FILE>
# -pargs="<PROGRAM_ARGS>"
# -dryrun
# -inputdir=<INPUT_DIRECTORY>
# -outputdir=<OUTPUT_DIRECTORY>

import os, sys, uuid, json, boto3
sys.path.append('../../global_utils/src/')
import module_utils
import file_utils
from argparse import ArgumentParser
from datetime import datetime

BATCH_SETTINGS_FILE = 'batch.settings.json'


def setJobProperties( module_name, batch_defaults_json, module_template_json):
    job_properties = {}
    job_properties['image'] = batch_defaults_json['ecr_registry'] + '/' + str(module_name) + ':' + str(module_template_json['module_version'])
    job_properties['vcpus'] = int(module_template_json['compute']['vcpus']) \
                                  if 'compute' in module_template_json and 'vcpus' in module_template_json['compute'] \
                                  else int(batch_defaults_json['vcpus'])
    job_properties['jobRoleArn'] = batch_defaults_json['aws_ecs_job_role']
    job_properties['memory'] = int(module_template_json['compute']['memory']) \
                                   if 'compute' in module_template_json and 'memory' in module_template_json['compute'] \
                                   else int(batch_defaults_json['memory'])
    # job_properties['mountPoints'] = batch_defaults_json['mountpoints']
    # job_properties['volumes'] = batch_defaults_json['volumes']
    return job_properties


def run_batchjob( args_json ):
    
    def createDependentIdList( jobid_list ):
        jobid_list_final=[]
        for jobid in jobid_list:
            if jobid != '':
                jobid_list_final.append({'jobId': jobid})
        return jobid_list_final

    def setContainerOverrides( WORKING_DIR, module_name, runargs_filepath ):
        mycommand = []
        mycommand += ['--module_name', module_name]
        mycommand += ['--run_arguments', runargs_filepath]
        mycommand += ['--working_dir', WORKING_DIR]
        return mycommand
    
    # get batch defaults
    batch_defaults_json = file_utils.loadJSON(BATCH_SETTINGS_FILE)
    
    # docker module to be run
    module_name = args_json['module']
    
    # module template
    module_template_file = module_utils.downloadModuleTemplate( module_name, os.getcwd() )
    module_template_json = file_utils.loadJSON(module_template_file)
    
    # unique ID for this job
    unique_id = str(uuid.uuid4())
    
    # convert command-line string of arguments into an IO JSON
    io_json = module_utils.createIOJSON(args_json)
    io_json_name = module_utils.getModuleRunNameID( module_name, unique_id, 'io_json' )
    file_utils.writeJSON( io_json, io_json_name )
    print('ARGS JSON: '+str(args_json))
    
    # upload IO JSON to module directory
    io_json_remote_folder = file_utils.uploadFile(io_json_name, module_utils.getModuleIODirectory( module_name ))
    io_json_remote_full_path = os.path.join(io_json_remote_folder, io_json_name)
    
    # initialize Batch boto3 client access
    print('\nSetting up boto3 client in {}...'.format(batch_defaults_json['aws_region']))
    client = boto3.client('batch', region_name=batch_defaults_json['aws_region'])
    
    # initialize job jobQueue and dependent IDs
    JOB_QUEUE = args_json['jobqueue'] if 'jobqueue' in args_json and args_json['jobqueue'] != '' else batch_defaults_json['jobqueue']
    DEPENDENT_IDS = createDependentIdList(args_json['dependentid'].split(',')) if 'dependentid' in args_json and args_json['dependentid'] != '' else []
    
    # set properties for this job
    job_properties = setJobProperties( module_name, batch_defaults_json, module_template_json )
    job_name = module_utils.getModuleRunNameID( module_name, unique_id, 'job_name' )
    print('Setting job properties for job: '+str(job_name))

    # get the date and time stamp right before we submit job
    job_submission_timestamp = str(datetime.now())
    
    # set input and compute parameters for job submission - save this job submission info
    job_overrides = {'command': setContainerOverrides(batch_defaults_json['working_dir'], module_name, io_json_remote_full_path)}
    job_json = {'container_overrides': job_overrides}
    job_json['jobqueue'] = JOB_QUEUE
    job_json['jobname'] = job_name
    job_json['job_submission_timestamp'] = job_submission_timestamp
    job_json_name = module_utils.getModuleRunNameID( module_name, unique_id, 'job_json' )
    file_utils.writeJSON( job_json, job_json_name )
    job_json_remote_folder = file_utils.uploadFile(job_json_name, module_utils.getModuleJobDirectory( module_name ))
    job_json_remote_fullpath = os.path.join(job_json_remote_folder, job_json_name)
    
    job_def_name = module_utils.getModuleRunNameID( module_name, unique_id, 'job_def' )
    jobid_final = ''    
    if not module_utils.isDryRun( args_json ):
        # register job definition
        job_def_response = client.register_job_definition( jobDefinitionName = job_def_name,
                                                           type='container',
                                                           retryStrategy={'attempts': 3},
                                                           containerProperties=job_properties)
        print('\nRegistering Job Definition: '+str(job_def_name))
        
        # submit job                                                                                             
        job_submit_response = client.submit_job( jobName = job_name,
                                                 jobQueue = JOB_QUEUE,
                                                 jobDefinition = job_def_name,
                                                 containerOverrides = job_overrides,
                                                 dependsOn=DEPENDENT_IDS)
        job_name_submitted = str(job_submit_response['jobName'])
        jobid_final = str(job_submit_response['jobId'])
        print('Job submitted: '+str(job_name_submitted))
        print('Job ID: '+jobid_final)
        print('JOB SUBMISSION SUCCESS!')
    else:
        print('\nDRY RUN: nothing formally submitted to Batch.')

    return_json={'jobid': jobid_final, 'jobqueue': JOB_QUEUE, 'run_arguments_file': io_json_remote_full_path, \
                 'joboverrides': job_overrides}
    print(str(return_json))
    print('\n<======================================================>\n')
    return return_json


if __name__ == '__main__':
    def error(self, message):
        sys.stderr.write('Usage: %s\n' % message)
        self.print_help()
        sys.exit(2)
    # print('USAGE: $ python run_batchjob.py --module <MODULE> --sampleid <SID> --input <IN> --output <OUT> -- inputdir <DIR> --outputdir <DIR> --pargs "<ARGS>" --dryrun')
    argparser = ArgumentParser()
    file_path_group = argparser.add_argument_group(title='Run batch job arguments')
    file_path_group.add_argument('--module', '-m', help='name of docker module', required=True)
    file_path_group.add_argument('--program_subname', '-sub', help='subprogram to run. e.g., "mpileup" to run samtools mpileup using samtools module.', required=False, default='')
    file_path_group.add_argument('--sampleid', '-sid', help='sample ID', required=False)
    file_path_group.add_argument('--input', '-i', help='input file(s), e.g. my_R1.fastq,my_R2.fastq', required=True)
    file_path_group.add_argument('--output', '-o', help='output file(s)', required=True)
    file_path_group.add_argument('--inputdir', '-idir', help='input dir', required=False)
    file_path_group.add_argument('--outputdir', '-odir', help='output dir', required=False)
    file_path_group.add_argument('--pargs', help='program arguments, in quotes', required=False)
    file_path_group.add_argument('--alternate_inputs', '-alti', help='alternate input file(s), e.g. my.bed,my.fasta', required=False, default='')
    file_path_group.add_argument('--alternate_outputs', '-alto', help='alterate output file(s)', required=False, default='')
    file_path_group.add_argument('--dryrun', help='dry run only', required=False, action='store_true')
    file_path_group.add_argument('--dependentid', help='dependent ID for batch job', required=False, default='')
    file_path_group.add_argument('--jobqueue', help='queue to submit batch job', required=False, default='')    
    runbatchjob_args = argparser.parse_args()
    run_batchjob( vars(runbatchjob_args) )
