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
    user_id = event['requestContext']['authorizer']['claims']['custom:userid'] if 'requestContext' in event and 'authorizer' in event['requestContext'] and 'claims' in event['requestContext']['authorizer'] and 'custom:userid' in event['requestContext']['authorizer']['claims'] and 'userid' not in event_body else ''
    
    if 'teamid' in event_body and 'userid' in event_body:
        input_json['userid'] = lambda_utils.getParameter( event_body, 'userid', '' )
        input_json['teamid'] = lambda_utils.getParameter( event_body, 'teamid', '' )
    else:
        input_json['userid'] = user_id
        input_json['teamid'] = team_id
        
    if input_json['teamid'] != '':
        input_json['table'] = lambda_utils.getParameter( event_body, 'table', '' )
        
        json_out = aws_s3_utils.get_json_object('s3://hubseq-db/{}/{}.json'.format(input_json['teamid'], input_json['table']))
        
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
