import json
import aws_s3_utils
import lambda_utils

def lambda_handler(event, context):

    # print input request
    print('INPUT EVENT: '+str(event))
    
    # parse POST body within event object - assume parameters are in 'body' key
    input_json = {}
    event_body = json.loads(event['body'])

    # make sure teamid matches the bucket requested - otherwise, forbidden
    team_id = event['requestContext']['authorizer']['claims']['custom:teamid']
    print('TEAMID: '+str(team_id))
    if 'path' in event_body and event_body['path'].startswith(team_id):
        input_json['path'] = lambda_utils.getS3path(lambda_utils.getParameter( event_body, 'path', '' ))
        input_json['searchpattern'] = lambda_utils.getParameter( event_body, 'searchpattern', '' )
        json_out = aws_s3_utils.list_objects(input_json['path'], input_json['searchpattern'])
    
        message_response = json.dumps(json_out, default=aws_s3_utils.dateConverter)
    
        response_obj = {}
        response_obj['statusCode'] = 200
        response_obj['headers'] = {'Access-Control-Allow-Origin': '*'}
        response_obj['headers']['Content-Type'] = 'application/json'
        response_obj['body'] = message_response
    else:
        message_response = 'Forbidden'
        response_obj = {}
        response_obj['statusCode'] = 403
        response_obj['headers'] = {'Access-Control-Allow-Origin': '*'}
        response_obj['headers']['Content-Type'] = 'application/json'
        response_obj['body'] = message_response        
    
    return response_obj
