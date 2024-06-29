
# import pika
# #import logging
# import os
# import sys
# import json
# import mysql.connector
# import threading
# # Configure logging
# #logging.basicConfig(level=logging.INFO)
# #logger = logging.getLogger(__name__)
# print("in consumer 2")
# SEND_QUEUE_NAME = "send_to_prishh"

# amqp_url = os.environ.get('AMQP_URL')  # Using .get() to avoid KeyError if AMQP_URL is not set
# url_params = pika.URLParameters(amqp_url)

# #logger.info("Connecting to RabbitMQ...")
# #connection = pika.BlockingConnection(url_params)
# #channel = connection.channel()

# # Declare the queue
# #queue_name = 'stock-management'
# #channel.queue_declare(queue=queue_name, durable=True)
# #logger.info(f"Declared queue '{queue_name}'")

# mysql_params ={
#     'host' : 'mysql' ,
#     'user' : 'root'  ,
#     'password' : 'Sreeja123' ,
#     'database' : 'inventory_management' 
# }

# mysql_conn = mysql.connector.connect(**mysql_params)
# mysql_cursor=mysql_conn.cursor()
# '''
# def receive_msg(ch, method, properties, body):
#     #logger.info("Received message:")
#     #logger.info(body.decode('utf-8'))
#     print('received msg : ', body.decode('utf-8'), file=sys.stderr)
#     #print('received msg : ', body.decode('utf-8'))
#     # Uncomment the following line to acknowledge the message
#     # ch.basic_ack(delivery_tag=method.delivery_tag)

#     # Insert received message into MySQL table
# '''




# def send_message_to_consumer_3(components_to_order):
#     try:
#         # Connect to RabbitMQ
#         connection = pika.BlockingConnection(url_params)
#         channel = connection.channel()

#         # Declare the queue
#         channel.queue_declare(queue=SEND_QUEUE_NAME, durable=True)

#         # Prepare the message payload
#         message_payload = {}
#         for component_id, quantity_needed in components_to_order:
#             # Fetch component name from the database
#             query = """SELECT component_name FROM components WHERE component_id = %s"""
#             mysql_cursor.execute(query, (component_id,))
#             component_name = mysql_cursor.fetchone()[0]
            
#             # Add component name and quantity needed to message payload
#             message_payload[component_name] = quantity_needed

#         # Send the message to consumer_3
#         channel.basic_publish(exchange='', routing_key=SEND_QUEUE_NAME, body=json.dumps(message_payload))

#         print("Message sent to consumer_3:", message_payload, file=sys.stderr)

#         # Close the connection
#         connection.close()
#     except Exception as e:
#         print(f"Error sending message to consumer_3: {e}", file=sys.stderr)


# def update_component_quantity(component_id, quantity_to_decrement):
#     try:
#         # Fetch component name
#         query = """SELECT component_name FROM components WHERE component_id = %s"""
#         mysql_cursor.execute(query, (component_id,))
#         component_name = mysql_cursor.fetchone()[0]

#         # Update the quantity of the component in the database
#         query = """UPDATE components SET quantity = quantity - %s WHERE component_id = %s"""
#         mysql_cursor.execute(query, (quantity_to_decrement, component_id))
#         mysql_conn.commit()

#         print(f"Quantity of {component_name} (component_id: {component_id}) decremented by {quantity_to_decrement}.", file=sys.stderr)
#     except Exception as e:
#         print(f"Error updating component quantity: {e}", file=sys.stderr)

# def check_component_availability(product_id):
#     try:
#         # Fetch product components and their quantities from the database
#         query = """SELECT component_id, quantity_needed FROM product_components WHERE product_id = %s"""
#         mysql_cursor.execute(query, (product_id,))
#         product_components = mysql_cursor.fetchall()

#         components_to_order = []  # List to store components that need to be ordered
#         flag = False  # Flag to track if any component needs to be ordered

#         # Check each component availability
#         for component_id, quantity_needed in product_components:
#             # Fetch component details from the database
#             query = """SELECT component_name, quantity FROM components WHERE component_id = %s"""
#             mysql_cursor.execute(query, (component_id,))
#             component_details = mysql_cursor.fetchone()

#             # If component is not available in sufficient quantity, print message and return
#             if component_details and component_details[1] < quantity_needed:
#                 #print(f"Not enough {component_details[0]} available. Need to call supplier.", file=sys.stderr)
#                 #return False
#                 print(f"Not enough {component_details[0]} available. Need to call supplier.", file=sys.stderr)
#                 components_to_order.append((component_id, quantity_needed))
#                 flag = True

            
#             else:
#                 # Update component quantity in the database
#                 update_component_quantity(component_id, quantity_needed)

#         if flag:
#             # Send message to consumer_3 with components to order
#             send_message_to_consumer_3(components_to_order)
#         else:
#             print("All components available. Done making one more product.", file=sys.stderr)
        
#         return not flag 
#         # All components available in sufficient quantity
#         #print("All components available. Done making one more product.", file=sys.stderr)
#         #return True

#     except Exception as e:
#         print(f"Error: {e}", file=sys.stderr)
#         return False

# def receive_msg(ch, method, properties, body):
#     print('Received message:', body.decode('utf-8'), file=sys.stderr)

#     try:
#         # Decode JSON message
#         message = json.loads(body.decode('utf-8'))

#         # Extract product ID from the message
#         product_id = message.get('product_id')

#         # Check component availability for the specified product
#         check_component_availability(product_id)

#     except Exception as e:
#         print(f"Error processing message: {e}", file=sys.stderr)

# def receive_message_from_consumer_3(ch, method, properties, body):
    
#     print('Received message from consumer_3:', body.decode('utf-8'), file=sys.stderr)

# '''
# try:
#     connection = pika.BlockingConnection(url_params)
#     channel = connection.channel()

#     channel.queue_declare(queue=RECEIVE_QUEUE_NAME, durable=True)
#     channel.basic_consume(queue=RECEIVE_QUEUE_NAME, on_message_callback=receive_message_from_consumer_3, auto_ack=True)

#     print("Waiting to consume messages from consumer_3...", file=sys.stderr)

#     channel.start_consuming()

# except KeyboardInterrupt:
#     print("Consumer interrupted", file=sys.stderr)
# '''



# try:
#     # Connect to RabbitMQ

#     def thread_function1(channel1):
#         channel1.start_consuming()
    
#     def thread_function2(channel2):
#         channel2.start_consuming()    

#     connection = pika.BlockingConnection(url_params)
#     channel1 = connection.channel()
#     channel2 = connection.channel()

#     # Declare the queue
#     queue_name = 'stock-management'
#     RECEIVE_QUEUE_NAME = "Send_to_sreeja"
#     channel1.queue_declare(queue=queue_name, durable=True)
#     channel2.queue_declare(queue=RECEIVE_QUEUE_NAME, durable=True)
    
#     # Start consuming messages
#     # threading.currentThread()
#     channel1.basic_consume(queue=queue_name, on_message_callback=receive_msg, auto_ack=True)
#     channel2.basic_consume(queue=RECEIVE_QUEUE_NAME, on_message_callback=receive_message_from_consumer_3, auto_ack=True)

#     thread1 = threading.Thread(target=thread_function1, args=(channel1,))
#     thread2 = threading.Thread(target=thread_function2, args=(channel2,))

#     # Start the threads
#     thread1.start()
#     thread2.start()

#     # Wait for both threads to finish
#     thread1.join()
#     thread2.join()

#     # Start consuming messages
    
    
#     print("Waiting to consume messages...", file=sys.stderr)

#     # Consume messages
    
   

# except KeyboardInterrupt:
#     print("Consumer interrupted", file=sys.stderr)
# finally:
#     # Close MySQL connection
#     mysql_conn.close()

#     # Close RabbitMQ connection
#     connection.close()

# '''
# try:
#     # Start consuming messages
#     channel.basic_consume(queue=queue_name, on_message_callback=receive_msg, auto_ack=True)
#     #logger.info("Waiting to consume messages...")
#     channel.start_consuming()
# except KeyboardInterrupt:
#     #logger.info("Consumer interrupted")
#     pass
# except Exception as e:
#    # logger.error(f"Error in consumer: {e}")
#    print(f"Error in consumer: {e}", file=sys.stderr)
# finally:
#     connection.close()
# '''

import pika
import os
import sys
import json
import mysql.connector
import threading
import time

print("in consumer 2")

SEND_QUEUE_NAME = "send_to_prishh"
amqp_url = os.environ.get('AMQP_URL')
url_params = pika.URLParameters(amqp_url)

mysql_params ={
    'host' : 'mysql' ,
    'user' : 'root'  ,
    'password' : 'root' ,
    'database' : 'inventory_management' 
}

mysql_conn = mysql.connector.connect(**mysql_params)
mysql_cursor=mysql_conn.cursor()

def send_message_to_consumer_3(components_to_order):
    try:
        connection = pika.BlockingConnection(url_params)
        channel = connection.channel()

        channel.queue_declare(queue=SEND_QUEUE_NAME, durable=True)

        message_payload = {}
        for component_id, quantity_needed in components_to_order:
            query = """SELECT component_name FROM components WHERE component_id = %s"""
            mysql_cursor.execute(query, (component_id,))
            component_name = mysql_cursor.fetchone()[0]
            if (component_name=="Graphics Card"):
                component_name="Graphics_Card"

            message_payload[component_name] = quantity_needed

        channel.basic_publish(exchange='', routing_key=SEND_QUEUE_NAME, body=json.dumps(message_payload))

        print("Message sent to consumer_3:", message_payload, file=sys.stderr)

        connection.close()
    except Exception as e:
        print(f"Error sending message to consumer_3: {e}", file=sys.stderr)


def update_component_quantity(component_id, quantity_to_decrement):
    try:
        query = """SELECT component_name FROM components WHERE component_id = %s"""
        mysql_cursor.execute(query, (component_id,))
        component_name = mysql_cursor.fetchone()[0]

        query = """UPDATE components SET quantity = quantity - %s WHERE component_id = %s"""
        mysql_cursor.execute(query, (quantity_to_decrement, component_id))
        mysql_conn.commit()

        print(f"Quantity of {component_name} (component_id: {component_id}) decremented by {quantity_to_decrement}.", file=sys.stderr)
    except Exception as e:
        print(f"Error updating component quantity: {e}", file=sys.stderr)

def check_component_availability(product_id):
    try:
        query = """SELECT component_id, quantity_needed FROM product_components WHERE product_id = %s"""
        mysql_cursor.execute(query, (product_id,))
        product_components = mysql_cursor.fetchall()

        components_to_order = []
        flag = False

        for component_id, quantity_needed in product_components:
            query = """SELECT component_name, quantity FROM components WHERE component_id = %s"""
            mysql_cursor.execute(query, (component_id,))
            component_details = mysql_cursor.fetchone()

            if component_details and component_details[1] < quantity_needed:
                print(f"Not enough {component_details[0]} available. Need to call supplier.", file=sys.stderr)
                components_to_order.append((component_id, quantity_needed))
                flag = True
            else:
                update_component_quantity(component_id, quantity_needed)

        if flag:
            send_message_to_consumer_3(components_to_order)
        else:
            print("All components available. Done making one more product.", file=sys.stderr)
        
        return not flag 

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return False

def receive_msg(ch, method, properties, body):
    print('Received message:', body.decode('utf-8'), file=sys.stderr)

    try:
        message = json.loads(body.decode('utf-8'))
        product_id = message.get('product_id')
        check_component_availability(product_id)

    except Exception as e:
        print(f"Error processing message: {e}", file=sys.stderr)

def receive_message_from_consumer_3(ch, method, properties, body1):
    print('Received message from consumer_3:', body1.decode('utf-8'), file=sys.stderr)
    connection = pika.BlockingConnection(url_params)
    channel = connection.channel()

    channel.queue_declare(queue="send_to_vaish", durable=True)
    channel.basic_publish(exchange='', routing_key="send_to_vaish", body=body1)




def consume_messages(channel, queue_name, callback):
    consumer_tag = f'{queue_name}_consumer_{threading.current_thread().name}' # Append thread name to make consumer tag unique
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True, consumer_tag=consumer_tag)

'''
try:
    connection = pika.BlockingConnection(url_params)
    channel1 = connection.channel()
    channel2 = connection.channel()

    queue_name = 'stock-management'
    RECEIVE_QUEUE_NAME = "Send_to_sreeja"
    channel1.queue_declare(queue=queue_name, durable=True)
    channel2.queue_declare(queue=RECEIVE_QUEUE_NAME, durable=True)

    thread1 = threading.Thread(target=consume_messages, args=(channel1, queue_name, receive_msg))
    thread2 = threading.Thread(target=consume_messages, args=(channel2, RECEIVE_QUEUE_NAME, receive_message_from_consumer_3))

    thread1.start()
    thread2.start()

    channel1.start_consuming()  # Start consuming once all setup is done

    thread1.join()
    thread2.join()

except KeyboardInterrupt:
    print("Consumer interrupted", file=sys.stderr)
finally:
    mysql_conn.close()
    connection.close()
'''
def consume_from_queue(queue_name, callback):
    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()


def send_heartbeat():
    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.queue_declare(queue='heartbeat-exchange',durable=True)
    #channel.basic_publish(exchange='',routing_key='heartbeat-exchange',body='heartbeat')
    channel.basic_publish(exchange='', routing_key='heartbeat-exchange', body="Heartbeat message",
                      properties=pika.BasicProperties(headers={"consumer_name": "consumer_2"}))
    print("sent heartbeat from consumer 2")
    connection.close()

def heartbeat_thread():
    i=0
    while i<3:
        send_heartbeat()
        time.sleep(2)
        i+=1

def callback1(ch, method, properties, body):
    print(f"Received from Queue 1: {body}")

def callback2(ch, method, properties, body):
    print(f"Received from Queue 2: {body}")

if __name__ == "__main__":
    thread1 = threading.Thread(target=consume_from_queue, args=("stock-management", receive_msg))
    thread2 = threading.Thread(target=consume_from_queue, args=("Send_to_sreeja", receive_message_from_consumer_3))
    thread3 = threading.Thread(target=heartbeat_thread)

    thread1.start()
    thread2.start()
    thread3.start()

    thread1.join()
    thread2.join()
    thread3.join()