import json
import run_pipeline
import lambda_utils
import db_utils

def lambda_handler(event, context):

    event_body = json.loads(event['body'])

    # get team id and user id from context
    team_id = event['requestContext']['authorizer']['claims']['custom:teamid'] if 'requestContext' in event and 'authorizer' in event['requestContext'] and 'claims' in event['requestContext']['authorizer'] and 'custom:teamid' in event['requestContext']['authorizer']['claims'] and 'teamid' not in event_body else ''
    user_id = event['requestContext']['authorizer']['claims']['custom:userid'] if 'requestContext' in event and 'authorizer' in event['requestContext'] and 'claims' in event['requestContext']['authorizer'] and 'custom:userid' in event['requestContext']['authorizer']['claims'] and 'userid' not in event_body else ''
    
    input_json = {}    
    input_json['pipeline'] = lambda_utils.getParameter( event_body, 'pipeline', 'rnaseq:mouse' )
    input_json['teamid'] = team_id # lambda_utils.getParameter( event_body, 'teamid', 'test' )
    input_json['userid'] = user_id # lambda_utils.getParameter( event_body, 'userid', 'test' )
    input_json['modules'] = lambda_utils.getParameter( event_body, 'modules', 'fastqc' )
    input_json['input'] = lambda_utils.getS3path(lambda_utils.getParameter( event_body, 'input', ''), team_id, '', 'true')
    input_json['runid'] = lambda_utils.getParameter( event_body, 'runid', '' )

    required_params = ['pipeline', 'modules', 'input', 'runid']
    for param in required_params:
        if param not in event_body:
            input_json['mock'] = True
            input_json['dryrun'] = True
    
    input_json['scratchdir'] = lambda_utils.getParameter( event_body, 'scratchdir', '/tmp/' )

    # include optional parameters in batch job call, if present
    optional_params = ['moduleargs', 'altinputs', 'altoutputs', 'jobqueue', 'output', 'mock', 'dryrun', 'sampleids']
    
    for param in optional_params:
        if param in event_body:
            if param in ['output']:
                input_json[param] = lambda_utils.getS3path(lambda_utils.getParameter(event_body, param, ''), team_id, '', 'true')
            elif param in ['altinputs', 'altoutputs']:
                input_json[param] = lambda_utils.getS3path(lambda_utils.getParameter(event_body, param, ''), team_id, '')
            elif param in ['moduleargs']:
                input_json[param] = lambda_utils.getS3path_args(lambda_utils.getParameter(event_body, param, ''), team_id, '')
            else:
                input_json[param] = lambda_utils.getParameter( event_body, param, '' )

    # run pipeline
    print('INPUT JSON TO RUN PIPELINE: '+str(input_json))
    json_out = run_pipeline.run_pipeline(input_json)
    
    # add new run to database - currently doing nothing w response
    new_run = [{"runid": input_json['runid'],
                "teamid": team_id,
                "userid": user_id,
		"pipeline_module": input_json['pipeline'],
                "date_submitted": lambda_utils.getParameter( event_body, 'submitted', '2022-06-07 08:30:00'),
                "status": "SUBMITTED"}]
    db_response_runs = db_utils.db_insert(team_id+'/runs', new_run)
    print('DB RESPONSE RUNS: '+str(db_response_runs))
    
    # go through each module (job) and record in jobs table (DB)
    new_jobs = []
    for _module in json_out:
        for _sample in json_out[_module]:
            new_jobid = json_out[_module][_sample]['job_id']
            new_jobs.append({"jobid": new_jobid,
                             "module": _module,
		             "runid": input_json['runid'],
                             "sampleid": _sample,
                             "teamid": team_id,
		             "userid": user_id,
                             "submitted": lambda_utils.getParameter( event_body, 'submitted', '2022-06-07 08:30:00'),
                             "status": "SUBMITTED"})
    db_response_jobs = db_utils.db_insert(team_id+'/jobs', new_jobs)
    print('DB RESPONSE JOBS: '+str(db_response_jobs))
    
    # return job iDs
    message_response = json.dumps(json_out)
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {"Access-Control-Allow-Origin": "*"}
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj
