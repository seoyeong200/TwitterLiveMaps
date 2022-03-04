import argparse
from datetime import datetime
import logging
import random
import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
# from apache_beam.transforms.window import FixedWindows
from google.oauth2 import service_account
import os
import sys
import json
import numpy as np
from google.cloud import bigquery as google_bigquery
import uuid
key_path = 'tweetdeck-320105-9820690ac16e.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path

credentials = service_account.Credentials.from_service_account_file(
    key_path, 
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
)
pipeline_options = PipelineOptions(streaming=True)
table_id = 'tweetdeck-320105:tweetdeck.twitter_data_gcp'
topic_id = "projects/tweetdeck-320105/topics/twitterdata"

table_spec = bigquery.TableReference(
    projectId = 'tweetdeck-320105',
    datasetId = 'tweetdeck',
    tableId = 'twitter_data_gcp'
)

def check_origin(json_data):
    topic = str(json_data['keyword_list'])
    status = None
    if "retweeted_status" in list(json_data.keys()):
        json_data = json_data['retweeted_status']
        status = 'retweeted'
    elif "quoted_status" in list(json_data.keys()):
        json_data = json_data['quoted_status']
        status = 'quoted'
    else:
        status = 'origin'
        pass
    
    return json_data,status, topic

class getUsableData(beam.DoFn):

    def process(self, element):

        json_data = json.loads(element)

        json_data,status,topic = check_origin(json_data)
        temp_json = {'id': None, 'created_at': None, 'text': None, 'retweet_count': None, 
        'favorite_count': None, 'author_name': None, 'author_screen_name': None, 'profile_image_url': None,
        'lat': None, 'long': None, 'keyword_list': None}

        temp_json['id'] = json_data['id_str']
        try:
            temp_json['lat'] = float(json_data['place']['bounding_box']['coordinates'][0][0][1])
        except:
            temp_json['lat'] = None
        try:
            temp_json['long'] = float(json_data['place']['bounding_box']['coordinates'][0][0][0])
        except:
            temp_json['long'] = None

        temp_json['created_at'] = datetime.strftime(datetime.strptime(json_data['created_at'],'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S')
        if status != 'origin':
            try:
                temp_json['text'] = json_data['extended_tweet']['full_text']
            except:
                temp_json['text'] = json_data['text']
        else:
            temp_json['text'] = json_data['text']
        temp_json['retweet_count'] = int(json_data['retweet_count'])
        temp_json['favorite_count'] = int(json_data['favorite_count'])
        temp_json['author_name'] = json_data['user']['name']
        temp_json['author_screen_name'] = json_data['user']['screen_name']
        temp_json['profile_image_url'] = json_data['user']['profile_image_url']
        temp_json['keyword_list'] = str(topic)

        return [temp_json]

class bigqueryUpdate(beam.DoFn):
    def process(self, element):
        unique_id = str(element['id'])
        element = [element]
        client = google_bigquery.Client(project='tweetdeck-320105')
        table = client.get_table('tweetdeck-320105.tweetdeck_gcp.tweetdeck_sample')

        errors = client.insert_rows_json(table, element, row_ids=[unique_id])
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

def sendQuery(element):
    element = [element]
    client = google_bigquery.Client(project='tweetdeck-320105')
    errors = client.insert_rows_json('tweetdeck.twitter_data_gcp', element)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
# schema = 'id:STRING, created_at:DATETIME, text:STRING, retweet_count:INTEGER, \
#     favorite_count: INTEGER, author_name: STRING, author_screen_name: STRING, '

with beam.Pipeline(options=pipeline_options) as pipeline:
    lines = (pipeline 
        | "Read from pubsub" >> io.ReadFromPubSub(topic=topic_id)
        | "Get Usable Data" >> beam.ParDo(getUsableData())
        | "Query Bigquery" >> beam.ParDo(bigqueryUpdate())   
        | beam.Map(print)
    )