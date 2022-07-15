
def lambda_handler(event, context):
    message = 'Hello from Lambda! Event object: {}'.format(str(event))
    return {
        'message' : message
    }
