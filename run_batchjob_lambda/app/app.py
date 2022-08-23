import json
import run_batchjob
import lambda_utils
import db_utils

def lambda_handler(event, context):

    # print input request
    print('INPUT EVENT: '+str(event))
    
    # parse POST body within event object - assume parameters are in 'body' key
    input_json = {}
    event_body = json.loads(event['body'])
    print("EVENT BODY: "+str(event_body))
    input_json['module'] = lambda_utils.getParameter( event_body, 'module', 'goqc' )
    input_json['input'] = lambda_utils.getS3path(lambda_utils.getParameter( event_body, 'input', 's3://hubseq-data/test/rnaseq/run_test1/david_go/davidgo.goterms.txt' ))
    input_json['output'] = lambda_utils.getS3path(lambda_utils.getParameter( event_body, 'output', 's3://hubseq-data/test/rnaseq/run_test1/goqc/'))

    # for now, instead of throwing an error with missing input parameters, return mock run output. Can have mock run output indicate that parameters are missing.
    if 'module' not in event_body or 'input' not in event_body or 'output' not in event_body:
        print("sending back mock output")
        input_json['mock'] = True
        input_json['dryrun'] = True

    input_json['scratchdir'] = lambda_utils.getParameter( event_body, 'scratchdir', '/tmp/' )
    
    # include optional parameters in batch job call, if present
    optional_params = ['sampleid', 'program_subname', 'inputdir', 'outputdir', 'pargs', 'alternate_inputs', \
                       'alternate_outputs', 'dependentid', 'jobqueue', 'teamid', 'userid', 'runid']
    
    for param in optional_params:
        if param in event_body:
            if param in ['inputdir', 'outputdir', 'alternate_inputs', 'alternate_outputs']:
                input_json[param] = lambda_utils.getS3path(lambda_utils.getParameter(event_body, param, ''))
            elif param in ['pargs']:
                input_json[param] = lambda_utils.getS3path_args(lambda_utils.getParameter(event_body, param, ''))
            else:
                input_json[param] = lambda_utils.getParameter( event_body, param, '' )
    
    # batch job returns a JSON that includes 'jobid'
    json_out = run_batchjob.run_batchjob(input_json)

    # add new job to database - currently doing nothing with DB response
    new_job = [{"jobid": json_out['jobid'],
                "module": input_json['module'],
                "runid": input_json['runid'],
                "sampleid": lambda_utils.getParameter( event_body, 'sampleid', '' ),
                "teamid": input_json['teamid'],
                "userid": input_json['userid'],
                "submitted": lambda_utils.getParameter( event_body, 'submitted', '2022-06-07 08:30:00'),
                "status": "SUBMITTED"}]
    db_response_jobs = db_utils.db_insert(input_json['teamid']+'/jobs', new_job)
    print('DB RESPONSE JOBS: '+str(db_response_jobs))
    
    # add new run to database - currently doing nothing w response
    new_run = [{"runid": input_json['runid'],
                "teamid": input_json['teamid'],
                "userid": input_json['userid'],
                "pipeline_module": input_json['module'],
                "date_submitted": lambda_utils.getParameter( event_body, 'submitted', '2022-06-07 08:30:00'),
                "status": "SUBMITTED"}]
    db_response_runs = db_utils.db_insert(input_json['teamid']+'/runs', new_run)
    print('DB RESPONSE RUNS: '+str(db_response_runs))
    
    message_response = json.dumps(json_out)
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {"Access-Control-Allow-Origin": "*"}
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj
