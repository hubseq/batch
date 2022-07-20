FROM public.ecr.aws/lambda/python:3.8

# Copy function code
COPY set_metadata_lambda/app/app.py ${LAMBDA_TASK_ROOT}
COPY src/global_utils/src/global_keys.py ${LAMBDA_TASK_ROOT}
COPY src/global_utils/src/file_utils.py ${LAMBDA_TASK_ROOT}
COPY src/global_utils/src/aws_s3_utils.py ${LAMBDA_TASK_ROOT}
COPY src/global_utils/src/lambda_utils.py ${LAMBDA_TASK_ROOT}
COPY src/batch.settings.json ${LAMBDA_TASK_ROOT}

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY set_metadata_lambda/app/requirements.txt .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.lambda_handler" ]
