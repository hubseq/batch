import json
import run_pipeline
import lambda_utils
import db_utils

def lambda_handler(event, context):

    input_json = {}
    event_body = json.loads(event['body'])
    input_json['pipeline'] = lambda_utils.getParameter( event_body, 'pipeline', 'rnaseq:mouse' )
    input_json['teamid'] = lambda_utils.getParameter( event_body, 'teamid', 'test' )
    input_json['userid'] = lambda_utils.getParameter( event_body, 'userid', 'test' )
    input_json['modules'] = lambda_utils.getParameter( event_body, 'modules', 'fastqc' )
    input_json['input'] = lambda_utils.getS3path(lambda_utils.getParameter( event_body, 'input', 's3://hubtenants/test/rnaseq/run_test1/fastq/rnaseq_mouse_test_tiny1_R1.fastq.gz,s3://hubtenants/test/rnaseq/run_test1/fastq/rnaseq_mouse_test_tiny1_R2.fastq.gz'))
    input_json['runid'] = lambda_utils.getParameter( event_body, 'runid', '' )
    
    required_params = ['pipeline', 'teamid', 'userid', 'modules', 'input', 'runid']
    for param in required_params:
        if param not in event_body:
            input_json['mock'] = True
            input_json['dryrun'] = True
    
    input_json['scratchdir'] = lambda_utils.getParameter( event_body, 'scratchdir', '/tmp/' )

    # include optional parameters in batch job call, if present
    optional_params = ['moduleargs', 'altinputs', 'altoutputs', 'jobqueue', 'output', 'mock', 'dryrun']
    
    for param in optional_params:
        if param in event_body:
            if param in ['output', 'altinputs', 'altoutputs']:
                input_json[param] = lambda_utils.getS3path(lambda_utils.getParameter(event_body, param, ''))
            elif param in ['moduleargs']:
                input_json[param] = lambda_utils.getS3path_args(lambda_utils.getParameter(event_body, param, ''))
            else:
                input_json[param] = lambda_utils.getParameter( event_body, param, '' )
    
    json_out = run_pipeline.run_pipeline(input_json)

    # add new run to database - currently doing nothing w response
    new_run = [{"runid": input_json['runid'],
                "teamid": input_json['teamid'],
                "userid": input_json['userid'],
		"pipeline_module": input_json['pipeline'],
                "date_submitted": lambda_utils.getParameter( event_body, 'submitted', '2022-06-07 08:30:00'),
                "status": "SUBMITTED"}]
    db_response_runs = db_utils.db_insert(input_json['teamid']+'/runs', new_run)
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
                             "teamid": input_json['teamid'],
		             "userid": input_json['userid'],
                             "submitted": lambda_utils.getParameter( event_body, 'submitted', '2022-06-07 08:30:00'),
                             "status": "SUBMITTED"})
    db_response_jobs = db_utils.db_insert(input_json['teamid']+'/jobs', new_jobs)
    print('DB RESPONSE JOBS: '+str(db_response_jobs))
    
    # return job iDs
    message_response = json.dumps(json_out)
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {"Access-Control-Allow-Origin": "*"}
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj
