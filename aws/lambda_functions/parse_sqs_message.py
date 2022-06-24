import json

# Parse SQS message
def lambda_handler(event, context):
  for record in event['Records']:
    body_info = record["body"]
    test_json = json.loads(body_info)
    for key,value in test_json['responsePayload'].items():
      print(f"{key}:{value}")
  

# Parse EventBridge message
def lambda_handler(event, context):
    for record in event['Records']:
        body_info = record["body"]
        test_json = json.loads(body_info)
        bucket = test_json["detail"]["bucket"]["name"]
        key = test_json["detail"]["object"]["key"]
        print(f"bucket : {bucket}, key : {key}")