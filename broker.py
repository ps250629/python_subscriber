
# import sys
# from asyncio.windows_events import NULL
from email.headerregistry import ContentTypeHeader
import paho.mqtt.client as mqtt
import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes 
import time,logging,sys

import json
# from time import sleep

class Message:
    #
    # intialize class
    #
    def __init__(self, msg):
        self._msg = msg

    def get_response_topic(self):
        return self._msg.properties.ResponseTopic

    def get_payload(self):
        return self._msg.payload.decode('utf-8')

    def get_topic(self) :
        return self._msg.message.topic

    def get_qos(self) :
        return self._msg.message.qos


class Broker:
    #
    # intialize class
    #
    def __init__(self):
        self._connected = False   
        self._new_msg_received = False   
        # self._msg = None   
        # self._msg2 = None   
        self._msg_list = []

    #
    # clear received message queue
    #
    def clear_received_msgs(self ):
        self._msg_list.clear()

    #
    # returns received message queue
    def get_received_msgs(self ):
          return  self._msg_list
          
    # event handlers
    #
    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        print('---- on_connect ----')
        print('--------------------')
        print('  flags: ',flags)
        print('  properties: ',properties)
        print('  reason code: ', reason_code)
        self._connected = True
        print('--------------------')

    def on_disconnect(self, client, userdata, reason_code, properties=None):
        print('---- on_disconnect ----')
        print('  properties: ',properties)
        print('  reason code: ', reason_code)
        self._connected = True
        print('--------------------')

    def on_message(self, client, userdata, message):
        # print('---- on_message ---- ')
        # print('  topic:         ', message.topic)
        # print('  payload:       ', str(message.payload))
        # print('  qos:           ', str(message.qos))

        print(f'// topic         : {message.topic}')
        try:
            print('// responseTopic: ', str(message.properties.ResponseTopic))
        except:
            print('// responseTopic: ')
        # print(f'// response topic: {response_topic}')

        try:
            source = ''
            for props in message.properties.UserProperty:
                prop = props[0]
                val = props[1]
                if prop == 'source':
                    source = val
            # property1 = str(message.properties.UserProperty[0])
            # if (property1 == 'source'):
            #     source = message.properties.UserProperty[1]
        except:
            source = ""   
        print(f'// source        : {source}')
        print(f'// payload       : ')
        print(f'{str(message.payload)}')
        # print(f'{message.payload}')
        print()

        # try:
        #     print('  contentType: ', str(message.properties.ContentType))
        # except:
        #     print('  contentType: ')
 
        self._new_msg_received = True
        # self._msg = str(message.payload)
        # self._msg2 = Message(message)

        self._msg_list.append(Message(message))
        # print('  num received messages: ', len(self._msg_list))
        
        # print(message.topic + " " + str(message.qos) + " " + str(message.payload))
        # print('--------------------')

    def on_publish(self, mqttc, obj, mid):
        print('---- on_publish -----')
        print("message ID(mid): " + str(mid))
        print('--------------------')

    # def on_subscribe(self, mqttc, obj, mid, granted_qos):
    #     print("Subscribed: " + str(mid) + " " + str(granted_qos))
    def on_subscribe(self, client, userdata, mid, granted_qos,properties=None):
        print('---- on_subscribe -----')
        print("Subscribed: " + str(mid) + " " + str(granted_qos))
        print('SUBSCRIBED')
        print('-----------------------')

    def on_log(self, mqttc, obj, level, string):
        print(string)

    #
    # API
    #
    def connect(self, ip, port ):
        print('------------------------------------------------------------')
        print('connnect( ip: ' + ip + ', port: '.format('%d', port) + ')')

        self.mqtt_client = mqtt.Client(client_id="pythonTestClient", protocol=mqtt.MQTTv5)
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_publish = self.on_publish

        properties=mqtt.Properties(PacketTypes.CONNECT)
        print("Setting session expiry interval")
        properties.SessionExpiryInterval=30 #set session expiry interval

        self.mqtt_client.connect(ip, clean_start=False,properties=properties)

        self.mqtt_client.loop_start()
        # wait for connect
        while not self._connected: #wait in loop
            time.sleep(1)
        self.mqtt_client.loop_stop()
        print('------------------------------------------------------------')

    def disconnect(self):
        print('------------------------------------------------------------')
        print('disconnect()')
        self.mqtt_client.disconnect()
        print('------------------------------------------------------------')

    def is_connected(self):
        return self._connected

    def subscribe(self, topic ):
        print("subscribing from topic: " + str(topic))
        # print("Subscribed: " + str(mid) + " " + str(granted_qos))
        # self.mqtt_client.subscribe("testtopic", qos=1, options=None, properties=None )
        # self.mqtt_client.subscribe(topic, 2)
        self.mqtt_client.subscribe(topic)
        return

    def unsubscribe(self, topic ):
        print("unsubscribing from topic: " + topic)
        # print("Subscribed: " + str(mid) + " " + str(granted_qos))
        # self.mqtt_client.subscribe("testtopic", qos=1, options=None, properties=None )
        self.mqtt_client.unsubscribe(topic) #.subscribe(topic, 2)
        return

    def publish(self, topic, response_topic='', payload=None, properties=None, content_type = 'application/json' ):

        properties1=mqtt.Properties(PacketTypes.PUBLISH)

        properties1.ContentType=content_type

        properties1.ResponseTopic=response_topic

        return self.mqtt_client.publish(topic=topic, payload=payload, qos=1, retain=False, properties=properties1)


    def readFile(self, filename ):
        ret_str = ''
        lines = ''
        with open(filename) as f:
            lines =  f.readlines()
            ret_str = ' '.join(str(item) for item in lines)
            f.close()
        return ret_str

    def publishFile(self, topic, filename, response_topic='', content_type = 'application/json') :
        print('------------------------------------------------------------')
        print('publishFile( topic(' + topic + ', file(' + filename + ')')
        payload = self.readFile(filename)
        print('  payload:')
        print(payload)

        # publish device server startup event...
        #
        mqttInfo = self.publish(topic, response_topic, payload=payload, content_type=content_type)

        if mqttInfo.rc != 0 :
            print("  Failed to send broker message")
            print('------------------------------------------------------------')
                    
            exit(-1)

        print("  Message sent successfully")
        print('------------------------------------------------------------')
                


    def get_message(self, num_seconds=5 ):
        print('------------------------------------------------------------')
        print('get_message()')

        self._new_msg_received = False

        self._msg = ''

        self.mqtt_client.loop_start()

        while num_seconds != 0 and not self._new_msg_received: #wait in loop

            time.sleep(1)

            num_seconds -= 1

            if self._new_msg_received:

                self.mqtt_client.loop_stop()

                # return self._msg
                return self._msg_list[len(self._msg_list)-1]

        self.mqtt_client.loop_stop()
        print('------------------------------------------------------------')

    def timeslice(self, num_seconds=1 ):

        self.mqtt_client.loop_start()

        while num_seconds != 0: #wait in loop

            time.sleep(1)

            num_seconds -= 1

            self.mqtt_client.loop_stop()
    

# def simple_test():
#     broker = Broker()

#     subscribe_topics = [("test1", 0), ("test2", 2)]

#     broker.connect("127.0.0.1", port=1883)

#     if not broker.is_connected()  :
#         print("Failed to connect to broker")
#         exit

# simple_test()
        
    # def subscribe(self, topic):

    # def scan_item_code(self, item_code: str, msg_id: int):
    #     json_body = {"Header": {"MessageID": msg_id},
    #                  "Operation": {
    #                      "Name": "Scan",
    #                      "ScanData": item_code,
    #                      "ScanDataLabel": item_code,
    #                      "ScanDataType": 110
    #                  }}
    #     payload = json.dumps(json_body)
    #     self.publish('scox/v1/retailer1/store2/terminal01/emulator/scannerdevice', payload=payload)

    # def stop_lane(self):
    #     json_body = {"event": "stopSCO", "params": {}}
    #     payload = json.dumps(json_body)
    #     self.publish('scox/v1/retailer1/store2/terminal01/commands/requests', payload=payload)
    #     sleep(5)

    # def start_lane(self):
    #     json_body = {"event": "startSCO", "params": {}}
    #     payload = json.dumps(json_body)
    #     self.publish('scox/v1/retailer1/store2/terminal01/commands/requests', payload=payload)
    #     sleep(20)
    # def set_weight_on_bag_scale(self, weight: int):
    #     msg = {
    #         "Header": {"MessageID": randint(1, 2000)},
    #         "Operation": {"Name": "SetWeight", "Weight": weight}}
    #     payload = json.dumps(msg)
    #     Broker().publish('scox/v1/retailer1/store2/terminal01/emulator/BagScale', payload)

    # def set_zero_weight_on_bag_scale(self):
    #     msg = {
    #         "Header": {"MessageID": randint(1, 2000)},
    #         "Operation": {"Name": "SendStatusUpdateEvent", "EventID": 13}}
    #     payload = json.dumps(msg)
    #     Broker().publish('scox/v1/retailer1/store2/terminal01/emulator/BagScale', payload)
