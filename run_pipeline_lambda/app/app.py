import json
import run_pipeline
import lambda_utils

def lambda_handler(event, context):

    input_json = {}
    event_body = json.loads(event['body'])
    input_json['pipeline'] = lambda_utils.getParameter( event_body, 'pipeline', 'rnaseq:mouse' )
    input_json['teamid'] = lambda_utils.getParameter( event_body, 'teamid', 'test' )
    input_json['userid'] = lambda_utils.getParameter( event_body, 'userid', 'test' )
    input_json['modules'] = lambda_utils.getParameter( event_body, 'modules', 'fastqc' )
    input_json['input'] = lambda_utils.getParameter( event_body, 'input', 's3://hubtenants/test/rnaseq/run_test1/fastq/rnaseq_mouse_test_tiny1_R1.fastq.gz,s3://hubtenants/test/rnaseq/run_test1/fastq/rnaseq_mouse_test_tiny1_R2.fastq.gz' )
    
    required_params = ['pipeline', 'teamid', 'userid', 'modules', 'input']
    for param in required_params:
        if param not in event_body:
            input_json['mock'] = True
            input_json['dryrun'] = True
    
    input_json['scratchdir'] = lambda_utils.getParameter( event_body, 'scratchdir', '/tmp/' )

    # include optional parameters in batch job call, if present
    optional_params = ['runid', 'moduleargs', 'altinputs', 'altoutputs', 'jobqueue', 'output', 'mock', 'dryrun']
    
    for param in optional_params:
        if param in event_body:
            input_json[param] = lambda_utils.getParameter( event_body, param, '' )
    
    json_out = run_pipeline.run_pipeline(input_json)

    # return job iDs
    message_response = json.dumps(json_out)
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {"Access-Control-Allow-Origin": "*"}
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj
