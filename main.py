import json
import paho.mqtt.client as mqtt
from opcua import Client
import time

OPC_SERVER_OUTPUT_URL = "opc.tcp://localhost:4840/freeopcua/server/"
NODES_TO_MONITOR = [
    "ns=2;i=1",  # Replace with your OPC UA node IDs to monitor
    "ns=2;i=2",
]

# MQTT Broker Info
MQTT_BROKER_ADDRESS = "mqtt.eclipse.org"
MQTT_BROKER_PORT = 1883
MQTT_TOPIC = "opc_data"
GL_SEND_DATA = True

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("$SYS/#")

# The callback for when a PUBLISHING message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

def main():
    #Connection to opc server
    client = Client(OPC_SERVER_OUTPUT_URL)
    client.connect()
    print("OPC server is connected")
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.connect(MQTT_BROKER_ADDRESS, MQTT_BROKER_PORT)
    mqttc.loop_start()
    try:
        while True:
            data = {}
            for node_id in NODES_TO_MONITOR:
                node = client.get_node(node_id)
                value = node.get_value()
                data[node_id] = value
            # Publish data to MQTT broker
            mqttc.publish(MQTT_TOPIC, json.dumps(data))
            time.sleep(1)  # Adjust interval as needed
    finally:
        client.disconnect()
        mqttc.loop_stop()
        mqttc.disconnect()
        print("Disconnected from OPC UA server and MQTT broker")


if __name__ == "__main__":
    main()
