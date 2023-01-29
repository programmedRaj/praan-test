from slurry_config import *
import paho.mqtt.client as mqtt
import time
import csv
import json

def on_connect(client, userdata, flags, rc):
    print("Result from connect: {}".format(mqtt.connack_string(rc)))
    # Check whether the result form connect is the CONNACK_ACCEPTED connack code
    if rc != mqtt.CONNACK_ACCEPTED:
        raise IOError("Couldn't establish a connection with the MQTT server")

def publish_value(client, topic, value):
    # Set the maximum number of inflight messages to 10
    client.max_inflight_messages_set(10)

    # Set the maximum number of queued messages to 100
    client.max_queued_messages_set(100)

    result = client.publish(topic=topic, payload=value, qos=2)
    return result

if __name__ == "__main__":
    client = mqtt.Client(protocol=mqtt.MQTTv311)
    client.on_connect = on_connect
    client.connect(host="broker.hivemq.com", port=1883)
    client.loop_start()
    print_message = "{}: {}"
    sleeper = 1 #sleep seconds

    listof_10=[]


    while True:
        with open('test_dataset_all.csv') as csvfile:
            reader=csv.reader(csvfile)
            readerc = 0
            for row in reader:
                if readerc % 10 ==0:
                    data = {"listof_10":listof_10}
                    data_string = json.dumps(data)
                    publish_value(client, timestamp_topic, data_string)
                    print(print_message.format(merged_topic, data_string))

                    listof_10=[]

                device_id = row[0]
                timestamp = row[1]
                windspeed = row[2]
                wind_direction = row[3]
                pm1_particle = row[4]
                pm25_particle = row[5]
                pm10_particle = row[6]

                print(print_message.format(timestamp_topic, timestamp))

                # Defineing a JSON object by TimeStamps as key for grafana visualization.
                data = {
                    timestamp : {
                        "device_id": device_id,
                        "windspeed": windspeed,
                        "wind_direction": wind_direction,
                        "pm1_particle": pm1_particle,
                        "pm25_particle": pm25_particle,
                        "pm10_particle": pm10_particle,
                    }
                }

                # Convert the JSON object to a string to send obbject at once instead of sendiing individually in different topics
                data_string = json.dumps(data)
                print(data_string)

                publish_value(client, timestamp_topic, data_string)   
                listof_10.append(data_string)
                time.sleep(sleeper)
                readerc +=1


        client.disconnect()
        client.loop_stop()