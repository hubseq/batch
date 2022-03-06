#
# run_pipeline
# 
# Runs a pipeline of docker modules.
# 
# Input arguments:
# --pipeline <WHICH_PIPELINE_TO_RUN> - e.g., dnaseq_targeted:human,rnaseq:mouse
# --modules <WHICH_MODULES_TO_RUN> - e.g., fastqc,bwamem_bam,mpileup
# --samples <LIST_OF_RUN:SAMPLES> - e.g., run1:sample1,run1:sample2,run2:sample1,run2:sample2
# --input <INITIAL_LIST_OF_INPUT_FILES> - e.g., s3://fastq/R1.fastq,s3://fastq/R2.fastq. Can also specify a directory and file type like, s3://fastq/^fastq
# --userid <USER_ID>
# --teamid <TEAM_ID>
# --moduleargs <LIST_OF_ARGS_FOR_EACH_MODULE> - e.g., '','-t S','','',... - LIST SAME SIZE as --modules. Cannot contain file paths
# --altinputs <LIST_OF_ALT_INPUTS_FOR_EACH_MODULE> - e.g., '','s3://fasta/hg38.fasta','',... LIST SAME SIZE AS --modules.
# --altoutputs <LIST_OF_ALT_OUTPUTS_FOR_EACH_MODULE> - e.g., '','s3://bed/out1.bed,s3://bed/out2.bed','',... LIST SAME SIZE AS --modules.
#

import os, sys, uuid, json, boto3
sys.path.append('../../global_utils/src/')
import module_utils
import file_utils
from argparse import ArgumentParser
from datetime import datetime
from run_batchjob import run_batchjob

def run_pipeline( args_json ):
    def getDAG( pipeline ):
        """ Creates a DAG adjacency dictionary from the pipeline DAG file.
        
        0>1:bcl2fastq
        1>2a:fastqc
        1>2b:bwamem,bwamem_bam
        2b>3:mpileup
        3>4:varscan2

        dependency_dag_dict = {‘1’: {‘dependent_modules’: [‘0’], 'dependency_type': 'linear', ‘modules’: [‘bcl2fastq’], ‘samples’’: {‘sample1’: [dependent_job_ids]:, ‘sample2’:  [depedent_job_ids],…,
                          {‘2a’: {‘dependent_modules’: [‘1’], ‘modules’: [‘fastqc’], ‘samples’: …
        dependency_dag_order = ['1', '2a',...]
        return depedency_dag_dict, depdency_order

        1>2:rnastar
        2+>3:deseq2
        """
        dag_order = []
        dependency_dag = {}
        with open(pipeline+'.dag.txt','r') as f:
            while True:
                r = f.readline()
                if r == '':
                    break
                elif r[0] == '#':
                    pass
                else:                    
                    r = r.rstrip(' \t\n')
                    # translate DAG edge into a dict: e.g., 1>2b:bwamem,bwamem_bam => {'2b': {...}
                    dag_edge = r.split(':')[0]  # assume no colon in docker module names
                    modules = r.split(':')[1].split(',')
                    dependent_modules = dag_edge.split('>')[0]
                    if dependent_modules[-1] == '+':
                        dependent_modules = dependent_modules[:-1].split(',')
                        dependency_type = 'multiin'
                    else:
                        dependent_modules = dependent_modules.split(',')
                        dependency_type = 'linear'
                    dag_node_id = dag_edge.split('>')[1]
                    dependency_dag[dag_node_id] = {'dependent_modules': dependent_modules, 'dependency_type': dependency_type, \
                                                   'modules': modules, 'samples': {}, 'sample_outputs': {}}
                    dag_order.append(dag_node_id)
        return dependency_dag, dag_order
    
    def getAlternateFiles( alt_file_args, module_template_file, alt_file_type ):
        """ Gets any alternate input or output files
        """
        alt_files = ''
        if module_args_list == '':
            alt_files = ''
        elif alt_file_type == 'altinput':
            if alt_file_args != '':
                # if alternate inputs are specified, get them from here
                alt_files = alt_file_args
            else:
                # otherwise get the defaults from the module template file
                alt_files = module_utils.getModuleTemplateDefaultAltInputs( module_template_file )

        elif alt_file_type == 'altoutput':
            if alt_file_args != '':
                alt_files = alt_file_args
            else:
                # otherwise get the defaults from the module template file
                alt_files = module_utils.getModuleTemplateDefaultAltOutputs( module_template_file )
        return alt_files
        
    def getOutputFilePath( docker_module, base_path, sample_id, output_file_name ):
        """ Gets the full path of the output file for this sample running this docker module.
            Output as list.
        """
        output_file_name = output_file_name.replace('<SAMPLE_ID>', sample_id)
        print('OUTPUT FILE NAME: '+str(output_file_name))
        full_path = file_utils.getFullPath(os.path.join(base_path, sample_id, docker_module), output_file_name.split(','))
        print('OUTPUT FULL PATH: '+str(full_path))
        returned_full_path = ''
        if type(full_path) == type([]):
            returned_full_path = ','.join(full_path)
        elif type(full_path) == type(''):
            if '.' in file_utils.getFileOnly(full_path):                
                returned_full_path = full_path      # output is a file
            else:
                returned_full_path = full_path+'/'  # output is a directory
        return returned_full_path
    
    def getDependencies( dag, dag_current_node, sample_id, dag_edge_type ):
        """ For the current module about to be run, gets the job IDs for the module jobs that need to finish before this can run
        """
        upstream_nodes = dag[dag_current_node]['dependent_modules']
        dependent_ids = []
        for upstream_node in upstream_nodes:
            if upstream_node == '0':  # will point to BCL files in the future
                pass
            elif dag_edge_type == 'linear':
                if sample_id in dag[upstream_node]['samples']:
                    dependent_ids.append(dag[upstream_node]['samples'][sample_id])
            elif dag_edge_type == 'multtin':
                for sample in dag[upstream_node]['samples']:
                    dependent_ids.append(dag[upstream_node]['samples'][sample])
        return dependent_ids

    def getPreviousOutput( dag, current_node, initial_output_files ):
        """ The input files of the current module are the output files of the previous module in the DAG,
        so we need a way to get this.
        """
        upstream_nodes = dag[current_node]['dependent_modules']
        sample_previous_output_files = {}
        for upstream_node in upstream_nodes:
            if upstream_node == '0':
                pass
            elif dag_edge_type == 'linear':
                for sample_id in dag[upstream_node]['sample_outputs']:
                    sample_previous_output_files[sample_id] = \
                        (sample_previous_output_files[sample_id] + [dag[upstream_node]['sample_outputs'][sample_id]]) \
                        if sample_id in sample_previous_output_files else [dag[upstream_node]['sample_outputs'][sample_id]]
            elif dag_edge_type == 'multtin':
                all_samples = []
                # each sample of current node module is dependent on ALL samples from previous module
                for sample_id in dag[upstream_node]['sample_outputs']:
                    all_samples.append(dag[upstream_node]['sample_outputs'][sample_id])
                for sample_id in dag[upstream_node]['sample_outputs']:                
                    sample_previous_output_files[sample_id] = \
                        (sample_previous_output_files[sample_id] + [all_samples]) \
                        if sample_id in sample_previous_output_files else [all_samples]
        # if there are no previous output files, then this is the initial module
        if sample_previous_output_files == {}:
            sample_previous_output_files = initial_output_files
        return sample_previous_output_files
    
    def createInputJSON( module, sampleid, input_files, output_files, alt_input_files, alt_output_files, module_args, \
                         dependent_ids, jobqueue, isdryrun ):
        """ Given sample, I/O, module and job dependency information, create JSON to submit to run batch job
        """        
        input_json = {}
        input_json['module'] = module
        input_json["sampleid"] = sampleid
        input_json["input"] = ','.join(input_files) if type(input_files)==type([]) else input_files
        input_json["output"] = ','.join(output_files) if type(output_files)==type([]) else output_files
        if alt_input_files != '' and alt_input_files != []:
            input_json["alternate_inputs"] = alt_input_files
        if alt_output_files != '' and alt_output_files != []:
            input_json["alternate_outputs"] = alt_output_files            
        if module_args != '' and module_args != []:
            input_json["pargs"] = module_args
        if dependent_ids != '' and dependent_ids != []:
            input_json["dependentid"] = dependent_ids
        if jobqueue != '':
            input_json["jobqueue"] = jobqueue
        if isdryrun:
            input_json["dryrun"] = True
        return input_json
        
    # first parse the DAG file for the pipeline being run - this will determine module order
    # dag will looks like this:
    # [('linear_step', ['bcl2fastq']), ('optional_step', ['fastqc']), ('linear_step', ['bwamem', 'bwamem_bam']), ('linear_step', ['mpileup'])] 
    dag, dag_node_list = getDAG( args_json['pipeline'] )

    # initialize: get list of modules and module arguments the user wants to run, and initialize job dependencies
    module_list = args_json['modules'].split(',')
    module_args_list = args_json['moduleargs'].split(',') if ('moduleargs' in args_json and args_json['moduleargs'] != '') else []
    alt_input_list = args_json['altinputs'].split(',') if ('altinputs' in args_json and args_json['altinputs'] != '') else []
    alt_output_list = args_json['altoutputs'].split(',') if ('altoutputs' in args_json and args_json['altoutputs'] != '') else []    
    jobQueue = args_json['jobqueue'] if 'jobqueue' in args_json else ''
    isDryRun = True if ('dryrun' in args_json and args_json['dryrun'] == True) else False
    input_files = {}
    # initial input files REQUIRED - these will feed into first module. Has format {'sampleid': [files],...}
    initial_output_files = file_utils.groupInputFilesBySample(str(args_json['input']).split(','))
    
    print('DAG: '+str(dag))
    print('INITIAL INPUT FILES: '+str(initial_output_files))
    # now step through the DAGs and run any modules that appear in the module input list
    for dag_node in dag_node_list:
        dag_node_modules = dag[dag_node]['modules']
        dag_edge_type = dag[dag_node]['dependency_type']
        
        for docker_module in dag_node_modules:
            if docker_module in module_list:
                print('FOUND DOCKER MODULE: '+str(docker_module))
                # get module template file
                module_template_file = module_utils.downloadModuleTemplate( docker_module, os.getcwd() )
                
                # input_files of this docker are the output files of the previous docker
                input_files = getPreviousOutput( dag, dag_node, initial_output_files )
                print('INPUT FILES FOR {}'.format(docker_module) + str(input_files))
                output_files = {}            # reset output files
                
                # get index of docker module within modules list, docker module arguments, alt input and alt output files
                module_index = module_list.index(docker_module)
                
                module_arguments = module_args_list[module_index] if module_args_list not in [[],''] else ''
                alt_input_args = alt_input_list[module_index] if alt_input_list not in [[],''] else ''
                alt_output_args = alt_output_list[module_index] if alt_output_list not in [[],''] else ''                
                
                alt_input_files = getAlternateFiles( alt_input_args, \
                                                     module_template_file, \
                                                     'altinput' )
                alt_output_files = getAlternateFiles( alt_output_args, \
                                                      module_template_file, \
                                                      'altoutput' )                

                print('ALT INPUT FILES: '+str(alt_input_files))
                print('ALT OUTPUT FILES: '+str(alt_output_files))
                
                # create run batch job for each sample
                for run_sample in list(input_files.keys()):
                    
                    # get base path and sample id of this sample
                    base_path = file_utils.getRunBaseFolder( input_files[run_sample][0] \
                                                                if (type(input_files[run_sample])==type([]) and \
                                                                len(input_files[run_sample]) > 0) else \
                                                                input_files[run_sample])
                    sample_id = file_utils.inferSampleID( file_utils.getFileOnly(input_files[run_sample][0]) \
                                                          if (type(input_files[run_sample])==type([]) and \
                                                              len(input_files[run_sample]) > 0) else \
                                                          file_utils.getFileOnly(input_files[run_sample]))
                    print('INPUT FILES: '+str(input_files))
                    print('SAMPLEID: '+str(sample_id))
                    print('BASE PATH: '+str(base_path))
                    
                    # create output file paths
                    output_files[sample_id] = getOutputFilePath( docker_module, \
                                                                 base_path, \
                                                                 sample_id, \
                                                                 module_utils.getModuleTemplateDefaultOutput(module_template_file))
                    
                    # create JSON for inputs
                    job_input_json = createInputJSON( docker_module, sample_id, input_files[run_sample], output_files[run_sample], \
                                                      alt_input_files, alt_output_files, module_arguments, \
                                                      getDependencies( dag, dag_node, sample_id, dag_edge_type ), \
                                                      jobQueue, \
                                                      isDryRun)
                    print('JOB_INPUT_JSON: '+str(job_input_json))
                    
                    # call runbatchjob()
                    job_output_json = run_batchjob( job_input_json )
                    print('JOB OUTPUT JSON: '+str(job_output_json))
                    
                    # for this sample, the next job in the DAG depends on this job
                    dag[dag_node]['sample_outputs'][sample_id] = output_files[sample_id]                    
                    if job_output_json != {} and 'jobid' in job_output_json and job_output_json['jobid'] != '':
                        dag[dag_node]['samples'][sample_id] = job_output_json['jobid']
    
                    
    return


if __name__ == '__main__':
    def error(self, message):
        sys.stderr.write('Usage: %s\n' % message)
        self.print_help()
        sys.exit(2)
    argparser = ArgumentParser()
    file_path_group = argparser.add_argument_group(title='Run batch pipeline arguments')
    file_path_group.add_argument('--pipeline', '-p', help='<WHICH_PIPELINE_TO_RUN> - e.g., dnaseq_targeted,rnaseq', required=True)
#    file_path_group.add_argument('--teamid', help='team ID for batch runs', required=True)
#    file_path_group.add_argument('--userid', help='user ID for batch runs', required=True)        
    file_path_group.add_argument('--modules', '-m', help='<WHICH_MODULES_TO_RUN> - e.g., fastqc,bwamem_bam,mpileup', required=True)    
#    file_path_group.add_argument('--samples', '-s', help='<LIST_OF_RUN:SAMPLES> - e.g., run1:sample1,run1:sample2,run2:sample1,run2:sample2', required=True)
    file_path_group.add_argument('--input', '-i', help='full path of initial INPUT_FILE(S) list - e.g., s3://fastq/R1.fastq,s3://fastq/R2.fastq. Can also specify a directory and file type to get all files of a file type in a dir, e.g. s3://fastq/^fastq or s3://fastq/* for all files.', required=True)    
    file_path_group.add_argument('--moduleargs', '-ma', help='list of program args for each module, in quotes - e.g., "","-t S","",... - LIST SAME SIZE as --modules. Cannot contain file paths', required=False, default='')
    file_path_group.add_argument('--altinputs', '-alti', help='alternate input file(s) for each module, e.g., "","s3://fasta/hg38.fasta","",... LIST SAME SIZE AS --modules. ', required=False, default='')
    file_path_group.add_argument('--altoutputs', '-alto', help='alterate output file(s) for each module', required=False, default='')
    file_path_group.add_argument('--dryrun', help='dry run only', required=False, action='store_true')
    file_path_group.add_argument('--jobqueue', help='queue to submit batch job', required=False, default='')    
    runpipeline_args = argparser.parse_args()
    run_pipeline( vars(runpipeline_args) )    
