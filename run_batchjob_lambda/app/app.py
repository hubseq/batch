import json
import run_batchjob
import lambda_utils

def lambda_handler(event, context):

    # parse POST body within event object - assume parameters are in 'data' key
    input_json = {}    
    input_json['module'] = lambda_utils.getParameter( event['data'], 'module', 'goqc' )
    input_json['input'] = lambda_utils.getParameter( event['data'], 'input', 's3://hubseq-data/test/rnaseq/run_test1/david_go/davidgo.goterms.txt' )
    input_json['output'] = lambda_utils.getParameter( event['data'], 'output', 's3://hubseq-data/test/rnaseq/run_test1/goqc/')

    # for now, instead of throwing an error with missing input parameters, return mock run output. Can have mock run output indicate that parameters are missing.
    if 'module' not in event['data'] or 'input' not in event['data'] or 'output' not in event['data']:
        input_json['mock'] = True
        input_json['dryrun'] = True

    input_json['scratchdir'] = lambda_utils.getParameter( event['data'], 'scratchdir', '/tmp/' )

    # include optional parameters in batch job call, if present
    optional_params = ['sampleid', 'program_subname', 'inputdir', 'outputdir', 'pargs', 'alternate_inputs', \
                       'alternate_outputs', 'dependentid', 'jobqueue']

    for param in optional_params:
        if param in event['data']:
            input_json[param] = lambda_utils.getParameter( event['data'], param, '' )
    
    # input_json['module'] = "goqc"
    # input_json['input'] = "s3://hubseq-data/test/rnaseq/run_test1/david_go/davidgo.goterms.txt"
    # input_json['output'] = "s3://hubseq-data/test/rnaseq/run_test1/goqc/"
    # input_json['sampleid'] = "goqc_test"
    # input_json['scratchdir'] = '/tmp/'        
    # input_json['mock'] = True
    # input_json['dryrun'] = True
    
    json_out = run_batchjob.run_batchjob(input_json)
    
    message = 'Hello from Lambda! Run job. Jerry is here. Here was the call: {}. Here are job IDs: {}'.format(str(input_json), str(json_out))
    message_response = json.dumps({'message': message})
    print(message)
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {}
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj
