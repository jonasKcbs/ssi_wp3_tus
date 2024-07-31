import os
import sys
import pika
from pika.exchange_type import ExchangeType
import json
import csv
import signal
import concurrent.futures
import params
from subprocess import STDOUT, check_output, CalledProcessError
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

class GeoRequest:
    data = None
    id = None
    geopoints = None
    geolocations = None
    settings = None

    def __init__(self, json_body):
        json_body = json_body.decode() # avoid failure in case of byte-like object
        if json_body == '':
            raise Exception("empty json body")
        self.data = json.loads(json_body)
        if not isinstance(self.data, dict):
            raise Exception("json not an object")
        
        required_keys = ['settings','id','geopoints','geolocations','metadata']
        if not all(k in self.data for k in required_keys):
            raise Exception("a key is missing")

        self.id = self.data['id']
        self.geopoints = self.data['geopoints']
        self.geolocations = self.data['geolocations']
        self.settings = self.data['settings']
        self.metadata = self.data['metadata']

        required_settings_keys = ['accuracy_meter','temporal_threshold_seconds','spatial_threshold_meter','start_new_cluster_meter']        
        if not all(k in self.settings for k in required_settings_keys):
            raise Exception("a key in 'settings' is missing")

    def path_geopoints(self):
        return  "/tmp/geoservice-" + self.id + "-geopoints.csv"

    def path_geolocations(self):
        return  "/tmp/geoservice-" + self.id + "-geolocations.csv"

    def path_clusters(self):
        return  "/tmp/geoservice-" + self.id + "-clusters.csv"
    
    def path_mapping(self):
        return  "/tmp/geoservice-" + self.id + "-mapping.csv"
    
    def body2csv(self):
        if len(self.geopoints):
            df = pd.DataFrame.from_dict(self.geopoints)
            df.to_csv(self.path_geopoints(), encoding='utf-8', index=False)
        else:
            with open(self.path_geopoints(), 'w') as outcsv:
                writer = csv.writer(outcsv)
                writer.writerow(['id','t','lon','lat','acc','metadata'])

        if len(self.geolocations):
            df = pd.DataFrame.from_dict(self.geolocations)
            df.to_csv(self.path_geolocations(), encoding='utf-8', index=False)
        else:
            with open(self.path_geolocations(), 'w') as outcsv:
                writer = csv.writer(outcsv)
                writer.writerow(['id','lon','lat','radius','metadata'])

    
    def has_geopoints(self):
        return self.geopoints and len(self.geopoints) > 0

class GeoTransportModePrediction:
    mode2priority = {
        "vehicle": 0,
        "bicycle": 1,
        "foot": 2,
        "still": 3,
        "unknown": 4
    }

    transistorsoft_mode2supported_mode = {
        "in_vehicle": "vehicle",
        "on_bicycle": "bicycle",
        "running": "foot",
        "on_foot": "foot",
        "walking": "foot",
        "still": "still",
        "unknown": "unknown"
    }

    def __init__(self):
        pass
    
    def key_compare(self, mode):
        return self.mode2priority[mode]
    
    def process(self, transistorsoft_input: list) -> list:
        distinctSet = set(transistorsoft_input)
        modes = [self.transistorsoft_mode2supported_mode[m] for m in distinctSet]
        return sorted(set(modes),key=self.key_compare)

class GeoService:
    geomsg = None

    def __init__(self, geomsg):
        self.geomsg = geomsg

    def add_transportmode(self, clusters, geopoints):
        prev_cluster_id = -1
        cluster2activities = ['unknown' for i in range(len(clusters))] # note that not all clusters have tracking points
        activities = []
        for p in geopoints:
            cluster_id = p['cluster_id']
            if cluster_id != prev_cluster_id:
                if prev_cluster_id != -1:
                    cluster2activities[prev_cluster_id] = GeoTransportModePrediction().process(activities)
                    activities = []
            if p['metadata'] != None:
                metadata = json.loads(p['metadata'])
                activities.append(metadata['transistorsoft_MotionActivityType'])
            else:
                activities.append('unknown')
            prev_cluster_id = cluster_id
        cluster2activities[prev_cluster_id] = GeoTransportModePrediction().process(activities)

        i = 0
        for c in clusters:
            c['transport_mode'] = cluster2activities[i]
            i += 1
    
        return clusters
    
    def process(self):
        if self.geomsg.has_geopoints():
            # write to csv for R
            self.geomsg.body2csv()

            s = self.geomsg.settings

            # execute command
            options = "-t " + str(s['temporal_threshold_seconds']) + " -s " + str(s['spatial_threshold_meter']) + \
                " -c " + str(s['start_new_cluster_meter']) + " -a " + str(s['accuracy_meter'])
            cmd = "cd /home/geo/code/R && /usr/bin/Rscript --vanilla process.R " + \
                options + " " + self.geomsg.path_geopoints() + " " + self.geomsg.path_geolocations() + " " + self.geomsg.path_clusters() + " " + self.geomsg.path_mapping()
            try:
                output = check_output(cmd, shell=True, stderr=STDOUT, timeout=params.process_timeout_seconds)
            except CalledProcessError as error:
                print('CalledProcessError:')
                print(error.cmd)
                print(error.output)

            # read clusters
            df = pd.read_csv(self.geomsg.path_clusters()) #,keep_default_na=False)
            df['cluster_id'] = df['cluster_id'].apply(lambda x: x - 1)
            df['index_start'] = df['index_start'].apply(lambda x: x if x == -1 else x - 1)
            df['index_end'] = df['index_end'].apply(lambda x: x if x == -1 else x - 1)
            clusters = df.to_dict('records')

            # read tracking points mapped to cluster id
            df = pd.read_csv(self.geomsg.path_mapping()) #,keep_default_na=False, na)
            df = df.replace(np.nan, None)
            df['cluster_id'] = df['cluster_id'].apply(lambda x: x - 1)
            convert_dict = {'id': str}
            df = df.astype(convert_dict)
            geopoints = df.to_dict('records')

            clusters = self.add_transportmode(clusters, geopoints)
        else:
            clusters = []
            geopoints = []

        result = {}
        result['settings'] = self.geomsg.settings
        result['id'] = self.geomsg.id
        result['clusters'] = clusters
        result['geopoints'] = geopoints
        result['metadata'] = self.geomsg.metadata
        json_result = json.dumps(result, allow_nan=False)
        return json_result

def on_message(channel, method_frame, header_frame, body):
    print('message received: ')
    #print(body)
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    print('message acked')
    georeq = GeoRequest(body)
    print('request parsed: ')
    #print(georeq)
    geosvc = GeoService(georeq)
    print('geosvc setup done')
    result = geosvc.process()
    print('processing results: ')
    #print(result)
    channel.basic_publish(
        exchange=params.rabbitmq_exchange,
        routing_key=params.rabbitmq_queue_processed,
        body=result,
    )
    print('message published')

def main():
    channel = None
    while True:
        try:
            channel = create_channel()
        except Exception as e:
            print(e)
            time.sleep(1)
            continue
        
        try:
            channel.basic_consume(
                queue=params.rabbitmq_queue_process, on_message_callback=on_message
            )
            channel.start_consuming()
        except Exception as e:
            print(e)
            if channel:
                channel.stop_consuming()
                channel = None
            time.sleep(1)
    return 0

if __name__ == "__main__":
    sys.exit(main())
