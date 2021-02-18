from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from kafka import KafkaConsumer
import time
import getopt
import json
import sys

Consumer = KafkaConsumer('uplink.data', value_deserializer=lambda m: json.loads(m.decode('utf-8')))

Transport = RequestsHTTPTransport(
    url='http://localhost:5000/graphql',
    use_json=True,
)

MSG_SECTION_META    = '1'
MSG_SECTION_DATA    = '2'
MSG_META_VERSION    = '10'
MSG_META_TYPE       = '11'
MSG_META_SUBTYPE    = '12'
MSG_META_ID         = '13'
DATA_KEY_MEASUREMENTS   = '100'

TYPE_REPORT                 = 0
SUBTYPE_MOISTURE_REPORT     = 1
SUBTYPE_BATTERY_REPORT      = 2
SUBTYPE_TEMPERATURE_REPORT  = 3

MessageTypeMap = {
    SUBTYPE_MOISTURE_REPORT: "MOIST",
    SUBTYPE_TEMPERATURE_REPORT: "TEMP",
    SUBTYPE_BATTERY_REPORT: "BAT"
}


def MessageTypeToString(msg_type, msg_subtype):
    if msg_type is not TYPE_REPORT:
        return None

    try:
        return MessageTypeMap[msg_subtype]
    except KeyError:
        print("ERROR: No such Message subtype: {}".format(msg_subtype))
        return None


def MessageDataToQuery(data, id, msg_type, timestamp):
    print(id)
    print(msg_type)
    print(timestamp)
    print(data)
    return gql("""
    mutation {
      createMeasurement(data:""" + str(data) + """, sensorHash: \"""" + str(id) + """\", sensorType: \"""" + msg_type + """\",  createdOnModule: \"""" + timestamp + """\"){
        measurement {
          id
        }
      }
    }
    """)


def ProcessMessage(client, msg):
    # {"network": NETWORK_TTN, "dev_id": dev_id, "rssi": rssi, "snr": snr, "time": time, "data": payload})
    id = msg["dev_id"]
    datetime = msg["time"]
    payload = msg["data"]

    msg_type = payload[MSG_SECTION_META][MSG_META_TYPE]
    msg_stype = payload[MSG_SECTION_META][MSG_META_SUBTYPE]

    samples = payload[MSG_SECTION_DATA][DATA_KEY_MEASUREMENTS]

    print("ID: {} | Datetime: {} | Payload: {} | Msg type: {} | Msg Subtype: {} | Samples: {}".format(id,
                                                                                                      datetime,
                                                                                                      payload,
                                                                                                      msg_type,
                                                                                                      msg_stype,
                                                                                                      samples))
    msg_type_str = MessageTypeToString(msg_type, msg_stype)
    print(msg_type_str)

    if msg_type_str is None:
        return -1

    for s in samples:
        query = MessageDataToQuery(s, id, msg_type_str, datetime)
        print(client.execute(query))

    return 0


def main(argv):

    GqlClient = Client(
        transport=Transport,
        fetch_schema_from_transport=True,
    )

    while True:
        for msg in Consumer:
            print(msg.value)
            ProcessMessage(GqlClient, msg.value)


if __name__ == '__main__':
    main(sys.argv[0:])
