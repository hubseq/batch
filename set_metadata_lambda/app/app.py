import json
import aws_s3_utils
import lambda_utils

def lambda_handler(event, context):

    # print input request
    print('INPUT EVENT: '+str(event))
    
    # parse POST body within event object - assume parameters are in 'body' key
    input_json = {}
    response_obj = {}
    event_body = json.loads(event['body'])
    team_id = event['requestContext']['authorizer']['claims']['custom:teamid'] if 'requestContext' in event and 'authorizer' in event['requestContext'] and 'claims' in event['requestContext']['authorizer'] and 'custom:teamid' in event['requestContext']['authorizer']['claims'] and 'teamid' not in event_body else ''
    if 'objects' in event_body and team_id != '':
        input_json['objects'] = lambda_utils.getS3path(lambda_utils.getParameter( event_body, 'objects', '' ), team_id, '')
        # tags should be a dictionary
        input_json['tags'] = lambda_utils.getParameter( event_body, 'tags', None )
        if input_json['tags'] != None:
            json_out = aws_s3_utils.set_metadata(input_json['objects'], input_json['tags'])

        # return updated tags
        message_response = json.dumps(json_out)
    
        response_obj['statusCode'] = 200
        response_obj['headers'] = {"Access-Control-Allow-Origin": "*"}    
        response_obj['headers']['Content-Type'] = 'application/json'
        response_obj['body'] = message_response
    else:
        message_response = 'Forbidden'
        response_obj['statusCode'] = 403
        response_obj['headers'] = {'Access-Control-Allow-Origin': '*'}
        response_obj['headers']['Content-Type'] = 'application/json'
        response_obj['body'] = message_response        
    return response_obj
