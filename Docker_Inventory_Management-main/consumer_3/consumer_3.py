import pika
import mysql.connector
import json
import sys
from datetime import datetime, timedelta
import os 
import threading
import time

QUEUE_NAME_1 = "send_to_prishh"
QUEUE_NAME_2 = "Send_to_sreeja"

# Define SQL commands to create tables
'''
MYSQL_HOST = 'host.docker.internal'
MYSQL_PORT=3306
# RabbitMQ connection parameters
# RABBITMQ_HOST = 'localhost' #For local
RABBITMQ_HOST = 'rabbitmq' #For docker
RABBITMQ_PORT = 5672
QUEUE_NAME = 'inventory_item_creation'
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = os.environ.get('RABBITMQ_PORT', '5672')
current_date = datetime.now().date()
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME_1)
print("Connected to RabbitMQ")
'''

print("in consumer 3")
amqp_url = os.environ.get('AMQP_URL')  # Using .get() to avoid KeyError if AMQP_URL is not set
url_params = pika.URLParameters(amqp_url)

#logger.info("Connecting to RabbitMQ...")
connection = pika.BlockingConnection(url_params)
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue=QUEUE_NAME_1, durable=True)
current_date = datetime.now().date()

def callback(ch, method, properties, body):
    obj = json.loads(body)
    print(obj)
    comp = list(obj.keys())  # Get component names

    connection = mysql.connector.connect(
        host="mysql",
        user="root",
        password='root',
        database="inventory_management"
    )
    cursor = connection.cursor(dictionary=True)  # Ensure dictionary cursor to fetch data easily
    print("Connected to MySQL")
    
    query = "SELECT * FROM Request WHERE component IN ({})".format(', '.join(['%s'] * len(comp)))
    cursor.execute(query, comp)
    rows = cursor.fetchall()
    
    send_message = {}
    for row in rows:
        if row["component"] in obj and row["count"] > obj[row["component"]]:
            formatted_date = current_date.strftime('%Y-%m-%d')
            send_message[row["component"]] = formatted_date
        else:
            
            formatted_date= (current_date + timedelta(days=row["manufacture_days"])).strftime('%Y-%m-%d')
            send_message[row["component"]] = formatted_date
            # Get the current date
            
    print("sent:",send_message)
    
    channel.basic_publish(exchange='', routing_key=QUEUE_NAME_2, body=json.dumps(send_message))



def send_heartbeat():
    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.queue_declare(queue='heartbeat-exchange',durable=True)
    #channel.basic_publish(exchange='',routing_key='heartbeat-exchange',body='heartbeat')
    channel.basic_publish(exchange='', routing_key='heartbeat-exchange', body="Heartbeat message",
                      properties=pika.BasicProperties(headers={"consumer_name": "consumer_3"}))
    print("sent heartbeat from consumer_3")
    connection.close()

def heartbeat_thread():
    i=0
    while i<3:
        send_heartbeat()
        time.sleep(2)
        i+=1

heartbeat_thread = threading.Thread(target=heartbeat_thread)
heartbeat_thread.start()

channel.basic_consume(queue=QUEUE_NAME_1, on_message_callback=callback, auto_ack=True)
print('Waiting for messages.')
channel.start_consuming()
