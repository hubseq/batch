import json
import aws_s3_utils
import lambda_utils

def lambda_handler(event, context):

    # print input request
    print('INPUT EVENT: '+str(event))
    
    # parse POST body within event object - assume parameters are in 'body' key
    input_json = {}
    event_body = json.loads(event['body'])
    input_json['path'] = lambda_utils.getParameter( event_body, 'path', '' )
    input_json['searchpattern'] = lambda_utils.getParameter( event_body, 'searchpattern', '' )
    json_out = aws_s3_utils.list_objects(input_json['path'], input_json['searchpattern'])
    
    message_response = json.dumps(json_out, default=aws_s3_utils.dateConverter)
    # message = 'Hello from Lambda! List ojbects at path. Jerry is here. Here was the call: {}. Here are the objects: {}'.format(str(input_json), str(json_out))
    # message_response = json.dumps(json_out)
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {'Access-Control-Allow-Origin': '*'}
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj
