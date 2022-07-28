import json
import get_jobstatus
import lambda_utils
import aws_s3_utils

def lambda_handler(event, context):

    # print input request
    print('INPUT EVENT: '+str(event))
    
    # parse POST body within event object - assume parameters are in 'body' key
    input_json = {}
    event_body = json.loads(event['body'])
    input_json['jobs'] = lambda_utils.getParameter( event_body, 'jobs', 'c6cfb43f-ad41-4488-ad42-0f3cd751e790,76065d62-0454-4a98-ba13-538112b7eed7' )
    
    # for now, instead of throwing an error with missing input parameters, return mock run output. Can have mock run output indicate that parameters are missing.
    if 'jobs' not in event_body:
        input_json['mock'] = True
        input_json['dryrun'] = True
    
    input_json['scratchdir'] = lambda_utils.getParameter( event_body, 'scratchdir', '/tmp/' )

    # include optional parameters in batch job call, if present
    optional_params = []
    
    for param in optional_params:
        if param in event_body:
            input_json[param] = lambda_utils.getParameter( event_body, param, '' )
    
    json_out = get_jobstatus.get_jobstatus(input_json)

    message_response = json.dumps(json_out, default=aws_s3_utils.dateConverter)
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {}
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj
