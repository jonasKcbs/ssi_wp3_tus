import os
import sys
import pika
from pika.exchange_type import ExchangeType
import json
import signal
import concurrent.futures
import params

def create_channel(params):
    cred_params = pika.credentials.PlainCredentials(params.rabbitmq_user, params.rabbitmq_password)
    connection_params = pika.ConnectionParameters(
        host=params.rabbitmq_host, port=params.rabbitmq_port, virtual_host=params.rabbitmq_vhost_geo, credentials=cred_params, heartbeat=600, blocked_connection_timeout=300
    )
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    print(f"channel created: {connection_params}")
    channel.exchange_declare('exchange_geoservice', durable=True, exchange_type=ExchangeType.topic)
    print(f"exchange declared: {params.rabbitmq_exchange_geo}")
    channel.queue_declare(queue=params.rabbitmq_queue_process, auto_delete=False)
    channel.queue_bind(queue=params.rabbitmq_queue_process, exchange=params.rabbitmq_exchange_geo, routing_key=params.rabbitmq_queue_process)
    print(f"queue declared and bound: {params.rabbitmq_queue_process}")
    #channel.queue_declare(queue=params.rabbitmq_queue_processed, auto_delete=False)
    #channel.queue_bind(queue=params.rabbitmq_queue_processed, exchange=params.rabbitmq_exchange_geo, routing_key=params.rabbitmq_queue_processed)
    #print(f"queue declared and bound: {params.rabbitmq_queue_processed}")
    return channel

#def run_ocr_with_timeout(image, filename, injector, timeout_seconds=300):
    #with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        #future = executor.submit(process, image, filename, injector)
        #return future.result(timeout=timeout_seconds)

def on_message(channel, method_frame, header_frame, body):
    print('message received')
    print(method_frame)
    print(header_frame)
    print(body)
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    channel.basic_publish(
        exchange=params.rabbitmq_exchange_geo,
        routing_key=params.rabbitmq_queue_processed,
        body=body,
    )

def main():
    try:
        channel = create_channel(params)
        channel.basic_consume(
            queue=params.rabbitmq_queue_process, on_message_callback=on_message
        )
        channel.start_consuming()
    except Exception as e:
        print(e)
        channel.stop_consuming()

if __name__ == "__main__":
    sys.exit(main())
