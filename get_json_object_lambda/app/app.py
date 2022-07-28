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
    json_out = aws_s3_utils.get_json_object(input_json['objects'])

    # get json object back
    message_response = json.dumps({'data': json_out})
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {}
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj
