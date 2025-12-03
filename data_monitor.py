import paho.mqtt.client as paho
from paho import mqtt
import time
import json
import csv
import os

CSV_FILE = "data.csv"

mqtt_username = "" # Insira seu nome de usuário MQTT
mqtt_password = "" # Insira sua senha MQTT
mqtt_broker = "" # Insira o endereço do broker MQTT

# Create CSV header if file doesn't exist
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "temperature", "humidity"])


def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)

def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_message(client, userdata, msg):
    local_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    payload = msg.payload.decode()

    print("Time: " + local_time + "\nPayload: " + payload)

    try:
        data = json.loads(payload)
        temp = data.get("temp")
        hum = data.get("hum")

        with open(CSV_FILE, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([local_time, temp, hum])

    except json.JSONDecodeError:
        print("Payload is not valid JSON.")


client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect

client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
client.username_pw_set(mqtt_username, mqtt_password)
client.connect(mqtt_broker, 8883)

client.on_subscribe = on_subscribe
client.on_message = on_message

client.subscribe("casa/sensor/#", qos=1)

try: 
    client.loop_forever()
except KeyboardInterrupt:
    print("Disconnected by user")
except Exception as exception:
    print("Error in loop:", exception)