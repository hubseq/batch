import json
import run_pipeline

def lambda_handler(event, context):

    input_json = {}
    input_json['pipeline'] = "rnaseq:mouse"
    input_json['modules'] = "deseq2,deqc,david_go,goqc"
    input_json['input'] = "s3://hubtenants/hubseq/test/runs/test-20220714-1633/expressionqc/expressionqc.counts_matrix.column.csv"
    input_json['altinputs'] = "'s3://hubtenants/<team_id>/<user_id>/runs/<run_id>/expressionqc/expressionqc.samplegroups.csv','','',''"
    input_json['moduleargs'] = "'','-pvaluecolumn pvalue','-cond pvalue<0.5,log2FoldChange>0.5',''"
    input_json['teamid'] = "hubseq"
    input_json['userid'] = "test"
    input_json['runid'] = "test-20220714-1633"
    input_json['scratchdir'] = '/tmp/'        
    # input_json['mock'] = True
    # input_json['dryrun'] = True
    
    json_out = run_pipeline.run_pipeline(input_json)
    
    message = 'Hello from Lambda! Jerry is here. Here are job IDs: {}'.format(str(json_out))
    message_response = json.dumps({'message': message})
    print(message)
    
    response_obj = {}
    response_obj['statusCode'] = 200
    response_obj['headers'] = {}
    response_obj['headers']['Content-Type'] = 'application/json'
    response_obj['body'] = message_response
    
    return response_obj
