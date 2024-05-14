import os
import sys
import pika
from pika.exchange_type import ExchangeType
import json
import signal
import concurrent.futures
import params
from subprocess import STDOUT, check_output
import pandas as pd
import numpy as np
import time

def create_channel():
    cred_params = pika.credentials.PlainCredentials(params.rabbitmq_user, params.rabbitmq_password)
    connection_params = pika.ConnectionParameters(
        host=params.rabbitmq_host, port=params.rabbitmq_port, virtual_host=params.rabbitmq_vhost, credentials=cred_params, heartbeat=600, blocked_connection_timeout=300
    )
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    print(f"channel created: {connection_params}")
    channel.exchange_declare(params.rabbitmq_exchange, durable=True, exchange_type=ExchangeType.topic)
    print(f"exchange declared: {params.rabbitmq_exchange}")
    channel.queue_declare(queue=params.rabbitmq_queue_process, auto_delete=False)
    channel.queue_bind(queue=params.rabbitmq_queue_process, exchange=params.rabbitmq_exchange, routing_key=params.rabbitmq_queue_process)
    print(f"queue declared and bound: {params.rabbitmq_queue_process}")
    #channel.queue_declare(queue=params.rabbitmq_queue_processed, auto_delete=False)
    #channel.queue_bind(queue=params.rabbitmq_queue_processed, exchange=params.rabbitmq_exchange, routing_key=params.rabbitmq_queue_processed)
    #print(f"queue declared and bound: {params.rabbitmq_queue_processed}")
    return channel

class GeoMessage:
    data = None
    id = None
    geopoints = None
    geolocations = None

    def __init__(self, json_body):
        json_body = json_body.decode() # avoid failure in case of byte-like object
        if json_body == '':
            raise Exception("empty json body")
        self.data = json.loads(json_body)
        if not isinstance(self.data, dict):
            raise Exception("json not an object")
        if 'id' in self.data.keys():
            self.id = self.data['id']
        else:
            raise Exception("key 'id' missing")
        if 'geopoints' in self.data.keys():
            self.geopoints = self.data['geopoints']
        else:
            raise Exception("key 'geopoints' missing")
        if 'geolocations' in self.data.keys():
            self.geolocations = self.data['geolocations']
        else:
            raise Exception("key 'geolocations' missing")

    def path_geopoints(self):
        return  "/tmp/geoservice-" + self.id + "-geopoints.csv"

    def path_geolocations(self):
        return  "/tmp/geoservice-" + self.id + "-geolocations.csv"

    def path_results(self):
        return  "/tmp/geoservice-" + self.id + "-results.csv"
    
    def path_results2(self):
        return  "/tmp/geoservice-" + self.id + "-results2.csv"
    
    def body2csv(self):
        df = pd.DataFrame.from_dict(self.geopoints)
        df.to_csv(self.path_geopoints(), encoding='utf-8', index=False)

        df = pd.DataFrame.from_dict(self.geolocations)
        df.to_csv(self.path_geolocations(), encoding='utf-8', index=False)
    
class GeoService:
    geomsg = None

    def __init__(self, geomsg):
        self.geomsg = geomsg
    
    def process(self):
        # write to csv for R
        self.geomsg.body2csv()

        # execute command
        cmd = "cd /home/geo/code/R && /usr/bin/Rscript --vanilla process.R " + self.geomsg.path_geopoints() + " " + self.geomsg.path_geolocations() + " " + self.geomsg.path_results() + " " + self.geomsg.path_results2()
        output = check_output(cmd, shell=True, stderr=STDOUT, timeout=params.process_timeout_seconds)

        # read clusters
        df = pd.read_csv(self.geomsg.path_results()) #,keep_default_na=False)
        df = df.replace(np.nan, None)
        clusters = df.to_dict('records')

        # read tracking points mapped to cluster id
        df = pd.read_csv(self.geomsg.path_results2()) #,keep_default_na=False, na)
        df = df.replace(np.nan, None)
        geopoints = df.to_dict('records')

        result = {}
        result['clusters'] = clusters
        result['geopoints'] = geopoints
        json_result = json.dumps(result, allow_nan=False)
        print(json_result)
        return json_result

def on_message(channel, method_frame, header_frame, body):
    print('message received')
    print(method_frame)
    print(header_frame)
    print(body)
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    geomsg = GeoMessage(body)
    geosvc = GeoService(geomsg)
    result = geosvc.process()
    channel.basic_publish(
        exchange=params.rabbitmq_exchange,
        routing_key=params.rabbitmq_queue_processed,
        body=result,
    )

def main():
    channel = None
    while channel == None:
        try:
            channel = create_channel()
        except Exception as e:
            print(e)
        time.sleep(1)
        
    try:
        channel.basic_consume(
            queue=params.rabbitmq_queue_process, on_message_callback=on_message
        )
        channel.start_consuming()
    except Exception as e:
        print(e)
        if channel:
            channel.stop_consuming()

if __name__ == "__main__":
    sys.exit(main())
