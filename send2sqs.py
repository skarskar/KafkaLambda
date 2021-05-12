import json
import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

s3Event = {
      "Records": [
        {
          "eventVersion": "2.1",
          "eventSource": "aws:s3",
          "awsRegion": "us-east-1",
          "eventTime": "",
          "eventName": "ObjectCreated:CompleteMultipartUpload",
          "userIdentity": {
            "principalId": "EXAMPLE"
          },
          "requestParameters": {
            "sourceIPAddress": ""
          },
          "responseElements": {
            "x-amz-request-id": "",
            "x-amz-id-2": ""
          },
          "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": "mb-raw-log-event-Prod",
            "bucket": {
              "name": "",
              "ownerIdentity": {
                "principalId": ""
              },
              "arn": ""
            },
            "object": {
              "key": "",
              "size": 0,
              "eTag": "",
              "sequencer": ""
            }
          }
        }
      ]
    }

taskResponse = {
      "invocationSchemaVersion": "",
      "treatMissingKeysAs" : "PermanentFailure",
      "invocationId" : "",
      "results": [
        {
          "taskId": "",
          "resultCode": "Succeeded",
          "resultString": ""
        }
      ]
    }

def lambda_handler(event, context):
    # TODO implement
    #populate s3Event
    s3Event["Records"][0]["s3"]["bucket"]["name"]= event['tasks'][0]["s3BucketArn"][13:]
    s3Event["Records"][0]["s3"]["bucket"]["arn"]=event['tasks'][0]["s3BucketArn"]
    s3Event["Records"][0]["s3"]["object"]["key"]=event['tasks'][0]["s3Key"]
    s3Event["Records"][0]["eventTime"]=datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    
    # Prepare results
    jobId = event['job']['id']
    taskResponse["invocationSchemaVersion"]=event['invocationSchemaVersion']
    taskResponse["invocationId"] = event['invocationId']
    taskResponse["results"][0]["taskId"] = event['tasks'][0]['taskId']
    taskResponse["results"][0]["resultString"] = json.dumps('200 OK')
    
    QueueName = 'testQ1' 
    # Set up logging
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(asctime)s: %(message)s')
    msg = send_sqs_message(QueueName,s3Event)
    print(msg)
    if msg is not None:
        logging.info(f'Sent SQS message ID: {msg["MessageId"]}')
                        
    return taskResponse


def send_sqs_message(QueueName, msg_body):
    """

    :param sqs_queue_url: String URL of existing SQS queue
    :param msg_body: String message body
    :return: Dictionary containing information about the sent message. If
        error, returns None.
    """

    # Send the SQS message
    sqs_client = boto3.client('sqs')
    sqs_queue_url = sqs_client.get_queue_url(QueueName=QueueName)['QueueUrl']
    print('sqs_queue_url ' + sqs_queue_url)
    try:
        msg = sqs_client.send_message(QueueUrl=sqs_queue_url,
                                      MessageBody=json.dumps(msg_body))
    except ClientError as e:
        logging.error(e)
        return None
    return msg
