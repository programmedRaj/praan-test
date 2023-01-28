from slurry_config import *
import paho.mqtt.client as mqtt
import time
import csv
def on_connect(client, userdata, flags, rc):
    print("Result from connect: {}".format(mqtt.connack_string(rc)))
    # Check whether the result form connect is the CONNACK_ACCEPTED connack code
    if rc != mqtt.CONNACK_ACCEPTED:
        raise IOError("Couldn't establish a connection with the MQTT server")
def publish_value(client, topic, value):
    result = client.publish(topic=topic, payload=value, qos=2)
    return result
if __name__ == "__main__":
    client = mqtt.Client(protocol=mqtt.MQTTv311)
    client.on_connect = on_connect
    client.connect(host="broker.hivemq.com", port=1883)
    client.loop_start()
    print_message = "{}: {}"
   
    while True:
        with open('test_dataset_all.csv') as csvfile:
            reader=csv.reader(csvfile)
            for row in reader:
                device_id = row[0]
                timestamp = row[1]
                windspeed = row[2]
                wind_direction = row[3]
                pm1_particle = row[4]
                pm25_particle = row[5]
                pm10_particle = row[6]
                print(print_message.format(wind_direction_topic, wind_direction))
                publish_value(client, device_id_topic, device_id)
                publish_value(client, timestamp_topic, timestamp)
                publish_value(client, windspeed_topic, windspeed)
                publish_value(client, wind_direction_topic, wind_direction)
                publish_value(client, pm1_particle_topic, pm1_particle)
                publish_value(client, pm25_particle_topic, pm25_particle)
                publish_value(client, pm10_particle_topic, pm10_particle)
                time.sleep(1)
 
        client.disconnect()
        client.loop_stop()