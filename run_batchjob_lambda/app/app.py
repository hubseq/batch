import json
import run_batchjob
import lambda_utils
import db_utils

def lambda_handler(event, context):

    # print input request
    print('INPUT EVENT: '+str(event))
    event_body = json.loads(event['body'])
    print("EVENT BODY: "+str(event_body))
    
    # get team id and user id from context
    team_id = event['requestContext']['authorizer']['claims']['custom:teamid'] if 'requestContext' in event and 'authorizer' in event['requestContext'] and 'claims' in event['requestContext']['authorizer'] and 'custom:teamid' in event['requestContext']['authorizer']['claims'] and 'teamid' not in event_body else ''
    user_id = event['requestContext']['authorizer']['claims']['custom:userid'] if 'requestContext' in event and 'authorizer' in event['requestContext'] and 'claims' in event['requestContext']['authorizer'] and 'custom:userid' in event['requestContext']['authorizer']['claims'] and 'userid' not in event_body else ''
    
    # parse POST body within event object - assume parameters are in 'body' key
    input_json = {}
    input_json['module'] = lambda_utils.getParameter( event_body, 'module', 'goqc' )
    input_json['input'] = lambda_utils.getS3path(lambda_utils.getParameter( event_body, 'input', 's3://hubseq-data/test/rnaseq/run_test1/david_go/davidgo.goterms.txt' ), team_id, '', 'true')
    input_json['output'] = lambda_utils.getS3path(lambda_utils.getParameter( event_body, 'output', 's3://hubseq-data/test/rnaseq/run_test1/goqc/'), team_id, '', 'true')
    
    # for now, instead of throwing an error with missing input parameters, return mock run output. Can have mock run output indicate that parameters are missing.
    if 'module' not in event_body or 'input' not in event_body or 'output' not in event_body or team_id == '':
        print("Error in input. Sending back mock output")
        if team_id == '':
            print("Error in team ID input. Sending back mock output.")
        input_json['mock'] = True
        input_json['dryrun'] = True

    input_json['scratchdir'] = lambda_utils.getParameter( event_body, 'scratchdir', '/tmp/' )
    
    # include optional parameters in batch job call, if present
    optional_params = ['sampleid', 'program_subname', 'inputdir', 'outputdir', 'pargs', 'alternate_inputs', \
                       'alternate_outputs', 'dependentid', 'jobqueue', 'teamid', 'userid', 'runid']
    
    for param in optional_params:
        if param in event_body:
            if param in ['inputdir', 'outputdir', 'alternate_inputs', 'alternate_outputs']:
                input_json[param] = lambda_utils.getS3path(lambda_utils.getParameter(event_body, param, ''), team_id, '')
            elif param in ['pargs']:
                input_json[param] = lambda_utils.getS3path_args(lambda_utils.getParameter(event_body, param, ''), team_id, '')
            else:
                input_json[param] = lambda_utils.getParameter( event_body, param, '' )
    
    # batch job returns a JSON that includes 'jobid'
    json_out = run_batchjob.run_batchjob(input_json)

    # get sample_id from event body if provided, otherwise get generated sampleid from batch job submission
    sampleid_body = lambda_utils.getParameter( event_body, 'sampleid', '' )
    sampleid_final = sampleid_body if sampleid_body != '' else (json_out['sampleid'] if 'sampleid' in json_out else '')
    
    # add new job to database - currently doing nothing with DB response
    new_job = [{"jobid": json_out['jobid'],
                "module": input_json['module'],
                "runid": input_json['runid'],
                "sampleid": sampleid_final,
                "teamid": input_json['teamid'] if 'teamid' in input_json else team_id,
                "userid": input_json['userid'] if 'userid' in input_json else user_id,
                # need to get date submitted...
                "submitted": lambda_utils.getParameter( event_body, 'submitted', '2022-06-07 08:30:00'),
                "status": "SUBMITTED"}]
    job_table = input_json['teamid']+'/jobs' if 'teamid' in input_json else team_id+'/jobs'
    db_response_jobs = db_utils.db_insert(job_table, new_job)
    print('DB RESPONSE JOBS: '+str(db_response_jobs))
    
    # add new run to database - currently doing nothing w response
    new_run = [{"runid": input_json['runid'],
                "teamid": input_json['teamid'] if 'teamid' in input_json else team_id,
                "userid": input_json['userid'] if 'userid' in input_json else user_id,
                "pipeline_module": input_json['module'],
                "date_submitted": lambda_utils.getParameter( event_body, 'submitted', '2022-06-07 08:30:00'),
                "status": "SUBMITTED"}]
    run_table = input_json['teamid']+'/runs' if 'teamid' in input_json else team_id+'/runs'    
    db_response_runs = db_utils.db_insert(run_table, new_run)
    print('DB RESPONSE RUNS: '+str(db_response_runs))
    
    message_response = json.dumps(json_out)
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {"Access-Control-Allow-Origin": "*"}
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj
