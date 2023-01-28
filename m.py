import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc):    
    print("Result from connect: {}".format(
            mqtt.connack_string(rc)))    
# Subscribe to the senors/praan/1 topic  
    client.subscribe("sensors/praan/1")                                 
def on_subscribe(client, userdata, mid, granted_qos):    
    print("I've subscribed")
def on_message(client, userdata, msg):
    try:
        print("Message received. Topic: {}. Payload: {}".format(msg.topic, str(msg.payload)))
    except:
        print(0)
if __name__ == "__main__":    
    client = mqtt.Client(protocol=mqtt.MQTTv311)    
    client.on_connect = on_connect    
    client.on_subscribe = on_subscribe    
    client.on_message = on_message    
    client.connect(host="broker.hivemq.com", port=1883)    
    client.loop_forever()