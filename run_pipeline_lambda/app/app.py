import json
import run_pipeline
import lambda_utils

def lambda_handler(event, context):

    input_json = {}
    event_body = json.loads(event_body)
    input_json['pipeline'] = lambda_utils.getParameter( event_body, 'pipeline', 'rnaseq:mouse' )
    input_json['teamid'] = lambda_utils.getParameter( event_body, 'teamid', 'test' )
    input_json['userid'] = lambda_utils.getParameter( event_body, 'userid', 'test' )
    input_json['modules'] = lambda_utils.getParameter( event_body, 'modules', 'fastqc' )
    input_json['input'] = lambda_utils.getParameter( event_body, 'input', 's3://hubtenants/test/rnaseq/run_test1/fastq/rnaseq_mouse_test_tiny1_R1.fastq.gz,s3://hubtenants/test/rnaseq/run_test1/fastq/rnaseq_mouse_test_tiny1_R2.fastq.gz' )
    
    required_params = ['pipeline', 'teamid', 'userid', 'modules', 'input']
    for param in required_params:
        if param not in event_body:
            input_json['mock'] = True
            input_json['dryrun'] = True
    
    input_json['scratchdir'] = lambda_utils.getParameter( event_body, 'scratchdir', '/tmp/' )

    # include optional parameters in batch job call, if present
    optional_params = ['runid', 'moduleargs', 'altinputs', 'altoutputs', 'jobqueue', 'output']
    
    for param in optional_params:
        if param in event_body:
            input_json[param] = lambda_utils.getParameter( event_body, param, '' )
    
    # input_json['pipeline'] = "rnaseq:mouse"
    # input_json['modules'] = "deseq2,deqc,david_go,goqc"
    # input_json['input'] = "s3://hubtenants/hubseq/test/runs/test-20220714-1633/expressionqc/expressionqc.counts_matrix.column.csv"
    # input_json['altinputs'] = "'s3://hubtenants/<team_id>/<user_id>/runs/<run_id>/expressionqc/expressionqc.samplegroups.csv','','',''"
    # input_json['moduleargs'] = "'','-pvaluecolumn pvalue','-cond pvalue<0.5,log2FoldChange>0.5',''"
    # input_json['teamid'] = "hubseq"
    # input_json['userid'] = "test"
    # input_json['runid'] = "test-20220714-1633"
    # input_json['scratchdir'] = '/tmp/'        
    # input_json['mock'] = True
    # input_json['dryrun'] = True
    
    json_out = run_pipeline.run_pipeline(input_json)
    
    message = 'Hello from Lambda! Jerry is here. Here is the input JSON: {}. Here are job IDs: {}'.format(str(input_json), str(json_out))
    message_response = json.dumps({'message': message})
    print(message)
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {}
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj