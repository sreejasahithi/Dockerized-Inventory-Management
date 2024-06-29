import pika
import time
import os
import threading

print("in consumer 4")
amqp_url = os.environ.get('AMQP_URL')  
url_params = pika.URLParameters(amqp_url)

last_heartbeats = {
    "consumer_1": 0,
    "consumer_2": 0,
    "consumer_3": 0
}

def callback(ch, method, properties, body):
    global last_heartbeats
    consumer_name = properties.headers.get("consumer_name")
    last_heartbeats[consumer_name] = time.time()
    print(" [x] Received heartbeat from", consumer_name)
    # current_time = time.time()
    # for consumer, last_heartbeat_time in last_heartbeats.items():
    #     time_since_last_heartbeat = current_time - last_heartbeat_time
    #     print(f"Current time: {current_time}, Last heartbeat time for {consumer}: {last_heartbeat_time}, Time since last heartbeat: {time_since_last_heartbeat}")
    #     if time_since_last_heartbeat > 4:
    #         print(f"Consumer {consumer} is not healthy.")

def check_consumer_health():
    while True:
        current_time = time.time()
        for consumer, last_heartbeat_time in last_heartbeats.items():
            if last_heartbeat_time != 0 :
                time_since_last_heartbeat = current_time - last_heartbeat_time
            #print(f"Current time: {current_time}, Last heartbeat time for {consumer}: {last_heartbeat_time}, Time since last heartbeat: {time_since_last_heartbeat}")
                if time_since_last_heartbeat > 4:
                    print(f"Consumer {consumer} is not healthy.")
        time.sleep(2)

def main():
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(url_params)
    channel = connection.channel()

    # Declare the exchange
    # channel.exchange_declare(exchange='heartbeat_exchange', exchange_type='fanout')

    # # Declare a queue with a random name
    # result = channel.queue_declare('', exclusive=True)
    # queue_name = result.method.queue


    # Bind the queue to the exchange
    # channel.queue_bind(exchange='heartbeat_exchange', queue=queue_name)

    channel.queue_declare(queue='heartbeat-exchange',durable=True)

    print(' [*] Waiting for heartbeats. To exit press CTRL+C')

    
    channel.basic_consume(queue='heartbeat-exchange', on_message_callback=callback, auto_ack=True)

    
    channel.start_consuming()

if __name__ == '__main__':
    # main()

    # Periodically check consumer health every 2 seconds
    # while True:
    #     time.sleep(2)
    #     check_consumer_health()
    callback_thread = threading.Thread(target=main)
    health_check_thread = threading.Thread(target=check_consumer_health)

    callback_thread.start()
    health_check_thread.start()

    callback_thread.join()
    health_check_thread.join()




# import pika
# import time
# import os
# import threading
# from flask import Flask, request, jsonify
# import requests

# app = Flask(__name__)

# print("in consumer 4")
# amqp_url = os.environ.get('AMQP_URL')  
# url_params = pika.URLParameters(amqp_url)

# # Dictionary to store the last heartbeat time of each consumer
# last_heartbeats = {
#     "consumer_1": 0,
#     "consumer_2": 0,
#     "consumer_3": 0
# }

# def callback(ch, method, properties, body):
#     global last_heartbeats
#     consumer_name = properties.headers.get("consumer_name")
#     last_heartbeats[consumer_name] = time.time()
#     print(" [x] Received heartbeat from", consumer_name)
#     #requests.post('http://localhost:5002/heartbeat', json={'consumer_name': consumer_name})
#     heartbeat_json = {'message': f"Received heartbeat from {consumer_name}"}
#     send_heartbeat_to_flask(heartbeat_json)

# def check_consumer_health():
#     while True:
#         current_time = time.time()
#         for consumer, last_heartbeat_time in last_heartbeats.items():
#             if last_heartbeat_time != 0:
#                 time_since_last_heartbeat = current_time - last_heartbeat_time
#                 if time_since_last_heartbeat > 4:
#                     print(f"Consumer {consumer} is not healthy.")
#                     health_status_json = {'message': f"Consumer {consumer} is not healthy."}
#                     send_health_status_to_flask(health_status_json)
#         time.sleep(2)

# def main():
#     # Connect to RabbitMQ
#     connection = pika.BlockingConnection(url_params)
#     channel = connection.channel()

#     # Declare the exchange
#     channel.queue_declare(queue='heartbeat-exchange', durable=True)

#     print(' [*] Waiting for heartbeats. To exit press CTRL+C')

#     # Consume heartbeats
#     channel.basic_consume(queue='heartbeat-exchange', on_message_callback=callback, auto_ack=True)

#     # Start consuming
#     channel.start_consuming()

# # def send_status_update(status):
# #     # Implement code to send status update to Flask endpoint
# #     print(f"Sending status update to Flask endpoint: {status}")

# # # Define a route in Flask to receive status updates
# # @app.route('/status', methods=['POST'])
# # def receive_status_update():
# #     status = request.json.get('status')
# #     print(f"Received status update from consumer: {status}")
# #     return 'OK'

# # # Define a route in Flask to receive heartbeat updates
# # @app.route('/heartbeat', methods=['POST'])
# # def receive_heartbeat():
# #     consumer_name = request.json.get('consumer_name')
# #     #last_heartbeats[consumer_name] = time.time()
# #     print(f"Received heartbeat from consumer: {consumer_name}")
# #     return 'OK'

# # @app.route('/heartbeat', methods=['GET'])
# # def receive_heartbeat():
# #     return jsonify({'message': 'Received heartbeat'})


# def send_heartbeat_to_flask(heartbeat_json):
#     url = 'http://localhost:5002/heartbeat'  # Update the URL accordingly
#     try:
#         response = requests.post(url, json=heartbeat_json)
#         response.raise_for_status()  # Raise an error for bad responses
#         print("Heartbeat sent successfully to Flask")
#     except requests.exceptions.RequestException as e:
#         print(f"Failed to send heartbeat to Flask: {e}")

# def send_health_status_to_flask(health_status_json):
#     url = 'http://localhost:5002/heartbeat'  # Update the URL accordingly
#     try:
#         response = requests.post(url, json=health_status_json)
#         response.raise_for_status()  # Raise an error for bad responses
#         print("Health status sent successfully to Flask")
#     except requests.exceptions.RequestException as e:
#         print(f"Failed to send health status to Flask: {e}")


# @app.route('/heartbeat', methods=['POST'])
# def receive_heartbeat():
#     heartbeat_data = request.json
#     send_heartbeat_to_flask(heartbeat_data)
#     return 'OK'

# @app.route('/health', methods=['POST'])
# def receive_health_status():
#     health_status_data = request.json
#     send_health_status_to_flask(health_status_data)
#     return 'OK'


# if __name__ == '__main__':
#     callback_thread = threading.Thread(target=main)
#     health_check_thread = threading.Thread(target=check_consumer_health)

#     callback_thread.start()
#     health_check_thread.start()

#     # Start Flask app in a separate thread
#     app.run(host='0.0.0.0', port=5002, debug=True)
