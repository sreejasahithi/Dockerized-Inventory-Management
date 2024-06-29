import pika
import os
import sys
import time
import mysql.connector
import json
import threading


# read rabbitmq connection url from environment variable
'''
amqp_url = os.environ['AMQP_URL']
url_params = pika.URLParameters(amqp_url)
# Connect to RabbitMQ server

while True:
    try:
        connection = pika.BlockingConnection(url_params)
        break
    except pika.exceptions.AMQPConnectionError:
        print("Error: RabbitMQ connection failed. Retrying in 5 seconds...")
        time.sleep(5)

channel = connection.channel()
channel.queue_declare(queue='order-processing')
'''
print("in consumer 1")
amqp_url = os.environ.get('AMQP_URL')  
url_params = pika.URLParameters(amqp_url)

#connection = pika.BlockingConnection(url_params)
#channel = connection.channel()
# queue_name = 'stock-management'
# channel.queue_declare(queue=queue_name, durable=True)


mysql_params ={
    'host' : 'mysql' ,
    'user' : 'root'  ,
    'password' : 'root' ,
    'database' : 'inventory_management' 
}

mysql_conn = mysql.connector.connect(**mysql_params)
mysql_cursor=mysql_conn.cursor()

'''
# Define a callback function to handle incoming messages
# def callback(ch, method, properties, body):
#     print(" [x] Received %r" % body)
#     # Insert received message into MySQL table
#     # mysql_cursor.execute("INSERT INTO messages (message) VALUES (%s)", (body.decode('utf-8'),))
#     # mysql_conn.commit()
    
#     # Select all records from the 'messages' table and print them
#     mysql_cursor.execute("SELECT * FROM products")
#     print("Messages in 'products' table:")
#     for row in mysql_cursor.fetchall():
#         print(row)

# Start consuming messages from the queue
# channel.basic_consume(queue='order-processing',
#                       on_message_callback=callback,
#                       auto_ack=True)
'''

product_id_global=None

def callback(ch, method, props, body):
    global product_id_global
    connection = pika.BlockingConnection(url_params)
    channel = connection.channel()
    queue_name = 'stock-management'
    channel.queue_declare(queue=queue_name, durable=True)

    print(" [x] Received %r" % body)
    #print(" [x] Reply-to property: %s" % properties.reply_to)
    # Convert the received JSON data to a dictionary
    data = json.loads(body)
    product_name = data.get('productName')
    location_address = data.get('locationAddress')

    # Check if the product exists in the products table
    #mysql_cursor.execute("SELECT product_id, product_name ,status ,due_date FROM products WHERE product_name = %s", (product_name,))
    mysql_cursor.execute("SELECT p.product_id, p.product_name, p.status, p.due_date FROM products p INNER JOIN locations l ON p.location_id = l.location_id WHERE p.product_name = %s AND l.address = %s", (product_name, location_address))
    product = mysql_cursor.fetchone()
    if product:
        # If product exists, send its ID and name back to the queue
        product_id, product_name ,status ,due_date= product
        if status == "manufactured" :
            product_id_global = product_id
            product_data = {'product_id': product_id, 'product_name': product_name , 'status': 'order satisfied'}
            product_data_json = json.dumps(product_data)

            channel.basic_publish(exchange='', routing_key=props.reply_to, 
                              properties=pika.BasicProperties(
                correlation_id=props.correlation_id,
            ),body= product_data_json)
            channel.basic_publish(exchange='', routing_key=queue_name,
                   body=product_data_json, properties=pika.BasicProperties(delivery_mode=2))  
        elif status == "in progress" :
            product_data = {'product_id': product_id, 'product_name': product_name , 'due_date': due_date.strftime("%Y-%m-%d")}
            product_data_json = json.dumps(product_data)
            channel.basic_publish(exchange='', routing_key=props.reply_to, 
                                  properties=pika.BasicProperties(
                    correlation_id=props.correlation_id,
                ),body= product_data_json)
    else :
        print(f"Product {product_name} not found in location {location_address}.", file=sys.stderr)
        message = {'error': f"Product {product_name} not found in location {location_address}."}
        channel.basic_publish(exchange='', routing_key=props.reply_to,
                              properties=pika.BasicProperties(
                                  correlation_id=props.correlation_id,
                              ), body=json.dumps(message))

    

# channel.basic_consume(queue='order-processing',
#                      on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')

# Enter a loop to wait for incoming messages
'''
try:
    # Enter a loop to wait for incoming messages
    channel.start_consuming()
except KeyboardInterrupt:
    # Gracefully close the connection on Ctrl+C
    connection.close()
'''
def consume_from_queue(queue_name, callback):
    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

# def callback1(ch, method, properties, body):
#     print("hello")
#     print(f"Received from Queue 2: {body.decode('utf-8')}")


def send_heartbeat():
    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.queue_declare(queue='heartbeat-exchange',durable=True)
    #channel.basic_publish(exchange='',routing_key='heartbeat-exchange',body='heartbeat')
    channel.basic_publish(exchange='', routing_key='heartbeat-exchange', body="Heartbeat message",
                      properties=pika.BasicProperties(headers={"consumer_name": "consumer_1"}))
    print("sent heartbeat from consumer 1")
    connection.close()

def heartbeat_thread():
    i=0
    while i<3:
        send_heartbeat()
        time.sleep(2)
        i+=1

def callback2(ch, method, properties, body):
    print("bye")

def callback1(ch, method, properties, body1):
    #print("hello")
    print('Received message from consumer_2:', body1.decode('utf-8'), file=sys.stderr)
    global product_id_global
    if product_id_global is not None:
        message=json.loads(body1.decode('utf-8'))
        component_due_dates=message
        due_dates=component_due_dates.values()
        max_due_date = max(due_dates,default=None)
        try:
            query = """UPDATE products SET due_date = %s, status = 'in progress' WHERE product_id = %s"""
            mysql_cursor.execute(query, (max_due_date, product_id_global))
            mysql_conn.commit()
            print(f"Updated due date to {max_due_date} and status to 'in progress' for product_id: {product_id_global}", file=sys.stderr)
        except Exception as e:
            print(f"Error updating due date and status: {e}", file=sys.stderr)


    
if __name__ == "__main__":
    thread1 = threading.Thread(target=consume_from_queue, args=("send_to_vaish", callback1))
    thread2 = threading.Thread(target=consume_from_queue, args=("order-processing", callback))
    thread3 = threading.Thread(target=heartbeat_thread)

    thread1.start()
    thread2.start()
    thread3.start()

    thread1.join()
    thread2.join()
    thread3.join()