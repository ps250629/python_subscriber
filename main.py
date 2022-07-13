import sys
from time import sleep
from broker import Broker

# print(f"size: {len(sys.argv)}")

BASE_TOPIC = 'scox/v1/NCR/store01/23174d56-b459-ea61-4578-d7359780313a'
TOPIC_INTERVENTIONS_REQUESTS =  BASE_TOPIC + '/interventions/requests'
TOPIC_INTERVENTIONS_REQUESTS_RESPONSES =  BASE_TOPIC + "/interventions/requests/#"
TOPIC_INTERVENTIONS_EVENTS =  BASE_TOPIC + '/interventions/events/#'
TOPIC_INTERVENTIONS_CURRENT_EVENTS =  BASE_TOPIC + '/interventions/current/events/#'
TOPIC_CARTS_EVENTS =  BASE_TOPIC + '/carts/events'
TOPIC_CARTS_REQUESTS =  BASE_TOPIC + '/carts/requests/#'
TOPIC_INTERVENTION_COMMAND_REQUEST = BASE_TOPIC + '/commands/intervention/requests'

TOPIC_POS_REQUESTS_SUB = BASE_TOPIC + '/pos/requests/#'

TOPIC_ITEMS_SALES_EVENTS_SUB = BASE_TOPIC + '/items/sales/events/#'

# TOPIC_POS_REQUESTS = BASE_TOPIC + '/pos/requests/#'
TOPIC_POS_REQUESTS = BASE_TOPIC + '/pos/requests'

TOPIC_ENDPOINTS_EVENTS = BASE_TOPIC + '/endpoints/events/#'
INTERVENTION_TOPICS = [
        (TOPIC_INTERVENTIONS_REQUESTS_RESPONSES, 1),
        (TOPIC_INTERVENTIONS_EVENTS, 1),
        (TOPIC_INTERVENTIONS_CURRENT_EVENTS, 1),
        (TOPIC_POS_REQUESTS_SUB, 1),
        (TOPIC_INTERVENTION_COMMAND_REQUEST, 1),
        (TOPIC_ITEMS_SALES_EVENTS_SUB,1),
        (TOPIC_CARTS_EVENTS, 1),
        (TOPIC_CARTS_REQUESTS,1)]


TOPIC_INTERVENTION_COMMAND_REQUEST = BASE_TOPIC + '/commands/intervention/requests'
TOPIC_INTERVENTION_COMMAND_REQUEST_RESPONSE = BASE_TOPIC + '/commands/intervention/requests/+'
START_COMMAND_TOPICS = [
        (TOPIC_INTERVENTION_COMMAND_REQUEST, 1),
        (TOPIC_INTERVENTION_COMMAND_REQUEST_RESPONSE, 1)]


TOPIC_INTERVENTION_STATES_REQUEST = BASE_TOPIC + '/sco/states/intervention/requests'
TOPIC_INTERVENTION_STATES_RESPONSE = BASE_TOPIC + '/sco/states/intervention/requests/+'
TOPIC_INTERVENTION_STATES_EVENTS = BASE_TOPIC + '/sco/states/events'
TOPIC_weight_state = BASE_TOPIC + '/interventions/delegates/weightsecurity/requests/+'

TOPIC_INTERVENTION_STATES = [
        (TOPIC_INTERVENTION_STATES_REQUEST, 1),
        (TOPIC_INTERVENTION_STATES_RESPONSE, 1),
        (TOPIC_INTERVENTION_STATES_EVENTS, 1),
        (TOPIC_weight_state,1)]


# subscribe_topic = INTERVENTION_TOPICS

subscribe_topic = TOPIC_INTERVENTION_STATES

broker = Broker()

broker.connect("127.0.0.1", port=8000)

if not broker.is_connected()  :
    print("Failed to connect to broker")
    exit

broker.subscribe(subscribe_topic)

broker.get_message(5)
broker.clear_received_msgs()

try:
    while True:
        broker.timeslice()
        # msgs = broker.get_message(1)
        # broker.clear_received_msgs()

        # for msg in msgs:
        #     printMessage(msg)
        # sleep(1)
except KeyboardInterrupt:
    pass    

