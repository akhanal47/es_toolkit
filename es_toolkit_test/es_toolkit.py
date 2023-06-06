import os
import sys
import requests
import json
from elasticsearch import Elasticsearch
from slack_sdk.webhook import WebhookClient
from dotenv import load_dotenv
load_dotenv()
import configparser

#this is to send the message to a slack webhook
def send_message(error_message='Error Parsing Document to JSON'):
    webhook_url =  os.getenv('SLACK_WEBHOOK')
    slack_webhook = WebhookClient(webhook_url)
    try:
       response = slack_webhook.send(text=str(error_message))
       return response.status_code
    except Exception as e:
        return e


#this is for creating an index
def create_index(endpoint, ELASTIC_USER, ELASTIC_PASSWORD, index_name, number_of_shards=1):
    #import requests
    headers = {
        'Content-Type': 'application/json',
                }
    
    #this defines the settings for the index created
    json_data = {
        'settings': {
            'number_of_shards': number_of_shards,
                    },
                }
    
    #the final endpoint for requests with the supplied index name
    endpoint_indexed=endpoint+'/'+index_name+'?pretty'

    #creating a put request to the elastic cluster for creating an index
    try:
        response = requests.put(endpoint_indexed, auth=(ELASTIC_USER, ELASTIC_PASSWORD), verify=False, headers=headers, json=json_data)
        status_code_received = response.status_code
        return status_code_received
    except Exception as e:
        return e
    
    #default return
    return None


#this is for the bulk indexing operations
def bulk_upload(json_files_folder, index_name='reads_publications'):
    #cluster password
    ELASTIC_USER = os.getenv('ELASTIC_USER')
    ELASTIC_PASSWORD = os.getenv('ELASTIC_PASSWORD')

    # Finger Certificate for communicating with the cluster, this is required for the HTTP requests authentication, user:password option is also avaiable
    CERT_FINGERPRINT = os.getenv('CERT_FINGERPRINT')

    #default values for port and host
    host = 'localhost'
    port = '9600'

    #defining the end point
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    endpoint = "https://"+host+":"+port

    #read the number of shards from a config file(this will be a init)
    try:
        config = configparser.ConfigParser()
        index_config_filename = index_name + '_' + 'config.ini'
        config.read(index_config_filename)
        number_of_shards = config.get('Settings', 'number_of_shards')
    except:
        number_of_shards = 1 
    
    #try creating index and uploading docs to the created index
    try:
        #to create an index

        create_index(endpoint, ELASTIC_USER, ELASTIC_PASSWORD, index_name, number_of_shards)
        elastic_client = Elasticsearch(
            endpoint,
            ssl_assert_fingerprint=CERT_FINGERPRINT,
            basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD)
        )

        #this defines the type of actions to perform during the bulk operations, the first is the action to perfomed followed by the body
        indexing_actions = []
        
        #read all the JSON files inside the specified folder
        for filename in os.listdir(json_files_folder):
            if filename.endswith('.json'):
                file_path = os.path.join(json_files_folder,filename)
                #print(file_path)
                with open(file_path,'r') as json_file:
                    json_data = json.load(json_file)
                    document_id = json_data['id']

                #check if a document with the same id already exists, if FALSE index the document, if TRUE log error
                # ! One better way to minimize the searching operations may be present, keep looking on the docs
                id_check = elastic_client.exists(index=index_name, id = document_id)
                id_check_response =  id_check.body

                #check if id exists already on the ES Index
                if id_check_response == False:
                    #if the id does not exist on the given index of the ES, define a indexing action for that ID and add it to the list of actions to perform
                    action = {"index": {"_index": index_name, "_id": document_id}}
                    indexing_actions.append(action)
                    #add body to the indexing actions
                    indexing_actions.append(json_data)
                
                #if the id exists just ignore the appending process, the file is skipped for indexing
                elif id_check_response == True:
                    pass
        try:
            elastic_client.bulk(index=index_name, operations=indexing_actions)
        except Exception as e:
            return e
    except Exception as e:
        return e