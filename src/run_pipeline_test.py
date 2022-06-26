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
sys.path.append('global_utils/src/')
import module_utils
import file_utils
from argparse import ArgumentParser
from datetime import datetime
from run_batchjob import run_batchjob

# SCRIPT_DIR = str(os.path.dirname(os.path.realpath(__file__)))

def run_pipeline( args_json ):
    print(str(args_json))
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
