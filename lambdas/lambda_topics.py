from botocore.vendored import requests
import base64
import json
import csv

def lambda_handler(event, context):
    
    buyingPlaces = ['Starbucks', 'Bloomingdales', 'Century21', 'Zara', 'H&M', 'Panera', 'Chipotle', 'Supreme']
    items1 = ['Tshirt', 'Coffee', 'Bread', 'Rice', 'Jeans', 'Pefume']
    items2 = ['Jacket', 'Blazer', 'Steak', 'Glares', 'Shoes', 'Overcoat', 'Lobster']
    
    
    for mVal in event['records']['CardTransRequest-0']:
        validTransaction = True
        message = mVal['value']
        decoded_message = base64.b64decode(message).decode('utf-8')
        
        transaction = decoded_message.split(':')
        if transaction[2] in items1 and int(transaction[3]) > 1000:
            validTransaction = False
        
        elif transaction[2] in items2 and int(transaction[3]) < 1000:
            validTransaction = False
            
        elif int(transaction[4]) > 1000:
            validTransaction = False
            
        if validTransaction:
            
            url = "http://ec2-54-202-63-16.us-west-2.compute.amazonaws.com:8082/topics/CardApproved"
            headers = {
            "Content-Type" : "application/vnd.kafka.json.v2+json"
            }
            # Create one or more messages
            # payload = {"records":[{"value":transaction[0]+","+transaction[1]+","+transaction[2]+","+transaction[3]}]}
            payload = {"records":[{"value":{"id": transaction[0], "buyingPlace": transaction[1], "item": transaction[2], "amount": transaction[3]}}]}
            #payload = {"records":[{"value":transaction[0]+","+transaction[1]+","+transaction[2]+","+transaction[3]}]}
            # Send the message
            r = requests.post(url, data=json.dumps(payload), headers=headers)
            if r.status_code != 200:
              print("Status Code: " + str(r.status_code))
              print(r.text)
              print(decoded_message, "APPROVED")
    
        else:
            
            url = "http://ec2-54-202-63-16.us-west-2.compute.amazonaws.com:8082/topics/CardDenied"
            headers = {
            "Content-Type" : "application/vnd.kafka.json.v2+json"
            }
            # Create one or more messages
            # payload = {"records":[{"value":transaction[0]+","+transaction[1]+","+transaction[2]+","+transaction[3]}]}
            payload = {"records":[{"value":{"id": transaction[0], "buyingPlace": transaction[1], "item": transaction[2], "amount": transaction[3]}}]}
            #payload = {"records":[{"value":transaction[0]+","+transaction[1]+","+transaction[2]+","+transaction[3]}]}
            # Send the message
            r = requests.post(url, data=json.dumps(payload), headers=headers)
            if r.status_code != 200:
              print("Status Code: " + str(r.status_code))
              print(r.text)
              print(decoded_message, "DENIED")

