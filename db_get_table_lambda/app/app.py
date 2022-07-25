import json
import aws_s3_utils
import lambda_utils

def lambda_handler(event, context):

    # print input request
    print('INPUT EVENT: '+str(event))
    
    # parse POST body within event object - assume parameters are in 'body' key
    input_json = {}
    event_body = json.loads(event['body'])
    input_json['userid'] = lambda_utils.getParameter( event_body, 'userid', '' )
    input_json['teamid'] = lambda_utils.getParameter( event_body, 'teamid', '' )
    input_json['table'] = lambda_utils.getParameter( event_body, 'table', '' )
    
    json_out = aws_s3_utils.get_json_object('s3://hubseq-db/{}/{}.json'.format(input_json['teamid'], input_json['table']))
    
    message = 'Hello from Lambda! Get table. Jerry is here. Here was the call: {}. Here are the table rows: {}'.format(str(input_json), str(json_out))
    message_response = json.dumps({'message': message})
    print(message)
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {}
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj
