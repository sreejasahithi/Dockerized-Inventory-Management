import pika
import os
import time
import sys
import mysql.connector
from flask import Flask, render_template, request, jsonify
import json
#import threading
import uuid


app = Flask(__name__,template_folder="templates")
# read rabbitmq connection url from environment variable
amqp_url = os.environ['AMQP_URL']
url_params = pika.URLParameters(amqp_url)

# connect to rabbitmq
connection = pika.BlockingConnection(url_params)
chan = connection.channel()

# declare a new queue
# durable flag is set so that messages are retained
# in the rabbitmq volume even between restarts
queue_name1='order-processing'
chan.queue_declare(queue=queue_name1, durable=True)

result = chan.queue_declare(queue='', exclusive=True)
callback_queue = result.method.queue
response_data= None

def on_response(ch, method, props, body):
    global response_data
    if props.correlation_id == props.correlation_id:
        #print("Received response:", body)
        response_data=body.decode()

chan.basic_consume(queue=callback_queue, on_message_callback=on_response, auto_ack=True)

mysql_params ={
    'host' : 'mysql' ,
    'user' : 'root'  ,
    'password' : 'vaish@123' ,
    'database' : 'inventory_management' 
}

mysql_conn = mysql.connector.connect(**mysql_params)
mysql_cursor=mysql_conn.cursor()

@app.route('/')
def index():
    return render_template("index.html")

@app.route('/submit', methods=['POST'])
def submit_form():
    global response_data 
    form_data = request.form.to_dict()

    json_data = json.dumps(form_data)

    correlation_id=str(uuid.uuid4())

    chan.basic_publish(exchange='',
                          routing_key=queue_name1,
                          properties=pika.BasicProperties(
                              reply_to=callback_queue,
                              correlation_id=correlation_id,
                          ),
                          body=json_data)
    
    
    print(" [x] Sent form data to consumer:", json_data)

    #while True:
    while response_data is None:
        connection.process_data_events()  # Process any incoming events
        time.sleep(0.1)
    
    response_dict = json.loads(response_data)
    temp_response_data = json.dumps(response_dict)
    response_data = None
    return jsonify({'message': 'Form data submitted successfully','response': temp_response_data}), 200

    #return render_template("submit.html", product_data=None)
'''
mysql_params ={
    'host' : 'mysql' ,
    'user' : 'root'  ,
    'password' : 'Sreeja123' ,
    'database' : 'sample_db' 
}

mysql_conn = mysql.connector.connect(**mysql_params)
mysql_cursor=mysql_conn.cursor()

mysql_cursor.execute("CREATE TABLE IF NOT EXISTS messages (id INT AUTO_INCREMENT PRIMARY KEY, message TEXT)")

for i in range(4):
    chan.basic_publish(exchange='', routing_key='hello',
                       body='Hello World', properties=pika.BasicProperties(delivery_mode=2))
    print("Produced the message")
'''    
print("sreeja")
# close the channel and connection
# to avoid program from entering with any lingering
# message in the queue cache
'''
chan.close()
connection.close()
'''

if __name__ == '__main__':
   app.run(host='0.0.0.0', port=5000, debug=True)