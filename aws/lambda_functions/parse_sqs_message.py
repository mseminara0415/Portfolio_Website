import json

def lambda_handler(event, context):
  for record in event['Records']:
    body_info = record["body"]
    test_json = json.loads(body_info)
    for key,value in test_json['responsePayload'].items():
      print(f"{key}:{value}")