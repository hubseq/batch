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
    # tags should be a dictionary
    input_json['tags'] = lambda_utils.getParameter( event_body, 'tags', None )
    if input_json['tags'] != None:
        json_out = aws_s3_utils.set_metadata(input_json['objects'], input_json['tags'])
    
    message = 'Hello from Lambda! Get metadata. Jerry is here. Here was the call: {}. Here is the metadata: {}'.format(str(input_json), str(json_out))
    message_response = json.dumps({'message': message})
    print(message)
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {}
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj
