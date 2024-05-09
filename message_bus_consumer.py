import os
import sys
import pika
from pika.exchange_type import ExchangeType
import json
import signal
import concurrent.futures
import params
from subprocess import STDOUT, check_output

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

def process():
    return check_output(params.command, shell=True, stderr=STDOUT, timeout=params.process_timeout_seconds)

def on_message(channel, method_frame, header_frame, body):
    print('message received')
    print(method_frame)
    print(header_frame)
    print(body)
    result = process()
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    channel.basic_publish(
        exchange=params.rabbitmq_exchange,
        routing_key=params.rabbitmq_queue_processed,
        body=body,
    )

def main():
    try:
        channel = create_channel()
    except Exception as e:
        print(e)
        print('failed to create channel')
        return 1
    try:
        channel.basic_consume(
            queue=params.rabbitmq_queue_process, on_message_callback=on_message
        )
        channel.start_consuming()
    except Exception as e:
        print(e)
        channel.stop_consuming()

if __name__ == "__main__":
    sys.exit(main())
