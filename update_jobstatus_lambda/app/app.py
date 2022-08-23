import json
import get_jobstatus
import lambda_utils
import aws_s3_utils
import db_utils

def lambda_handler(event, context):

    def combinedRunStatus( statusList ):
        # only SUCCEEDED if all succeeded
        if 'FAILED' in statusList:
            return 'FAILED'
        elif len(list(filter(lambda s: s=='SUCCEEDED', statusList)))==len(statusList):
            return 'SUCCEEDED'
        elif len(list(filter(lambda s: s in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTED', 'STARTING', 'RUNNING'], statusList))) > 0:
            return 'RUNNING'
        
    # print input request
    print('INPUT EVENT: '+str(event))
    
    # parse POST body within event object - assume parameters are in 'body' key
    input_json = {}
    response_obj = {}
    
    # JSON string or pure JSON
    if type(event) == type({}) and type(event['body']) == type({}):
        event_body = event['body']
    else:
        event_body = json.loads(event['body'])

    team_id = event['requestContext']['authorizer']['claims']['custom:teamid']
    if 'teamid' in event_body and event_body['teamid']==team_id:
        input_json['teamid'] = lambda_utils.getParameter( event_body, 'teamid', 'hubseq' )    
        input_json['scratchdir'] = lambda_utils.getParameter( event_body, 'scratchdir', '/tmp/' )

        # first get all job IDs from DB that are not currently finished.
        # this grabs all jobs - needs to be more secure later.
        db_all_jobs = db_utils.db_fetch(input_json['teamid']+'/jobs')
        runids_all = {}  # {<runid>: {<jobid>: status,...},...}
        jobs2runs = {}   # keep a dict of job IDs to run IDs
        print('DB ALL JOBS: '+str(db_all_jobs))
        jobs_to_update = ''
        for job_json in db_all_jobs:
            if 'status' in job_json and job_json['status'] not in ['SUCCEEDED', 'FAILED']:
                jobs_to_update += job_json['jobid']+','
            # job to run dict
            jobs2runs[job_json['jobid']] = job_json['runid']
            # keep track of job statuses for all run IDs
            if job_json['runid'] not in runids_all:
                runids_all[job_json['runid']] = {}
            if 'status' in job_json:
                runids_all[job_json['runid']][job_json['jobid']] = job_json['status']
    
        # now get the updated job status for these jobs
        json_out = get_jobstatus.get_jobstatus({"jobs": jobs_to_update.rstrip(',')})
        print('JOB STATUS JSON OUT: '+str(json_out))
        jobs2replace = []
        status2replace = []
        for job_response in json_out['jobs']:
            _jobid = job_response['jobId']
            _jobstatus = job_response['status']
            jobs2replace.append({"jobid": _jobid})
            status2replace.append({"status": _jobstatus})
            # update run ID status
            runids_all[jobs2runs[_jobid]][_jobid] = _jobstatus
    
        # update all runs
        runs2replace = []
        runstatus2replace = []
        for _runid in runids_all:
            # replace run statuses        
            runs2replace.append({"runid": _runid})
            runstatus2replace.append({"status": combinedRunStatus( list(runids_all[_runid].values()) )})
    
        # and do a replacement - hopefully this doesn't screw up
        aws_s3_utils.edit_json_object('s3://hubseq-db/{}/jobs.json'.format(input_json['teamid']), jobs2replace, status2replace)
    
        # update all run statuses as well
        aws_s3_utils.edit_json_object('s3://hubseq-db/{}/runs.json'.format(input_json['teamid']), runs2replace, runstatus2replace)
    
        message_response = json.dumps(json_out, default=aws_s3_utils.dateConverter)
    
        response_obj['statusCode'] = 200
        response_obj['headers'] = {"Access-Control-Allow-Origin": "*"}
        response_obj['headers']['Content-Type'] = 'application/json'
        response_obj['body'] = message_response
    else:
        message_response = 'Forbidden'
        response_obj['statusCode'] = 403
        response_obj['headers'] = {'Access-Control-Allow-Origin': '*'}
        response_obj['headers']['Content-Type'] = 'application/json'
        response_obj['body'] = message_response
        
    return response_obj
