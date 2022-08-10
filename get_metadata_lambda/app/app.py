import json
import aws_s3_utils
import lambda_utils

def lambda_handler(event, context):

    # print input request
    print('INPUT EVENT: '+str(event))
    
    # parse POST body within event object - assume parameters are in 'body' key
    input_json = {}
    event_body = json.loads(event['body'])
    input_json['objects'] = lambda_utils.getParameter( event_body, 'objects', '' )
    # input_json['scratchdir'] = lambda_utils.getParameter( event_body, 'scratchdir', '/tmp/' )
    json_out = aws_s3_utils.get_metadata(input_json['objects'])

    # respond with metadata tags - 
    message_response = json.dumps(json_out)
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {"Access-Control-Allow-Origin": "*"}    
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj
