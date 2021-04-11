from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from kafka import KafkaConsumer
from TimestampGen import TimestampGenA, TimestampGenB
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
MSG_META_ID         = '13'
MSG_META_VERSION    = '10'
MSG_META_TYPE       = '11'
MSG_META_SUBTYPE    = '12'
DATA_KEY_MEASUREMENTS   = '100'

TYPE_REPORT                 = 0
SUBTYPE_MOISTURE_REPORT     = 1
SUBTYPE_BATTERY_REPORT      = 2
SUBTYPE_TEMPERATURE_REPORT  = 3
SUBTYPE_GROWTH_REPORT       = 4

def HrToSec(hr):
    return 3600 * hr


TRANSMIT_INTERVAL = HrToSec(4)
TEMP_SAMPLE_INTERVAL = HrToSec(1)
BATTERY_SAMPLE_INTERVAL = HrToSec(24)
MOISTURE_SAMPLE_INTERVAL = HrToSec(4)


TempTimestampA = TimestampGenA(sample_interval_sec=TEMP_SAMPLE_INTERVAL,
                               transmit_interval_sec=TRANSMIT_INTERVAL)
BatteryTimestampA = TimestampGenA(sample_interval_sec=BATTERY_SAMPLE_INTERVAL,
                                  transmit_interval_sec=TRANSMIT_INTERVAL)
MoistureTimestampA = TimestampGenA(sample_interval_sec=MOISTURE_SAMPLE_INTERVAL,
                                   transmit_interval_sec=TRANSMIT_INTERVAL)


TempTimestampB = TimestampGenB(sample_interval_sec=TEMP_SAMPLE_INTERVAL,
                               transmit_interval_sec=TRANSMIT_INTERVAL)
BatteryTimestampB = TimestampGenB(sample_interval_sec=BATTERY_SAMPLE_INTERVAL,
                                  transmit_interval_sec=TRANSMIT_INTERVAL)
MoistureTimestampB = TimestampGenB(sample_interval_sec=MOISTURE_SAMPLE_INTERVAL,
                                   transmit_interval_sec=TRANSMIT_INTERVAL)

GrowthTimestampB = TimestampGenB(sample_interval_sec=MOISTURE_SAMPLE_INTERVAL,
                                   transmit_interval_sec=TRANSMIT_INTERVAL)

MessageTypeMap = {
    SUBTYPE_MOISTURE_REPORT: {
        "name": "MOIST",
        "timestamp_gen": {
            "A": MoistureTimestampA,
            "B": MoistureTimestampB
        }
    },

    SUBTYPE_TEMPERATURE_REPORT: {
        "name": "TEMP",
        "timestamp_gen": {
            "A": TempTimestampA,
            "B": TempTimestampB
        }
    },
    SUBTYPE_BATTERY_REPORT: {
        "name": "BAT",
        "timestamp_gen": {
            "A": BatteryTimestampA,
            "B": BatteryTimestampB
        }
    },
    SUBTYPE_GROWTH_REPORT: {
        "name": "GROWTH",
        "timestamp_gen": {
            "A": None,
            "B": GrowthTimestampB
        }
    }
}


def MessageTypeToString(msg_type, msg_subtype):
    if msg_type is not TYPE_REPORT:
        return None

    try:
        return MessageTypeMap[msg_subtype]["name"]
    except KeyError:
        print("ERROR: No such Message subtype: {}".format(msg_subtype))
        return None


def GenerateTimestamp(msg_type, msg_subtype, receive_datetime, num_samples):
    if msg_type is not TYPE_REPORT:
        return None

    try:
        gen = MessageTypeMap[msg_subtype]["timestamp_gen"]["B"]
        gen.SetNumberOfSamples(num_samples=num_samples)
        gen.SetReceiveTimestamp(receive_datetime)

        timestamp_b = gen.Next()

        print("Timestamp: {}".format(timestamp_b))

        return timestamp_b

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
    id = msg["meta"]["network"]["dev_id"]
    datetime = msg["meta"]["network"]["rx_time"]
    payload = msg["data"]

    msg_type = payload[MSG_SECTION_META][MSG_META_TYPE]
    msg_stype = payload[MSG_SECTION_META][MSG_META_SUBTYPE]

    samples = payload[MSG_SECTION_DATA][DATA_KEY_MEASUREMENTS]

    print("ID: {} | Datetime: {} "
          "| Payload: {} "
          "| Msg type: {} "
          "| Msg Subtype: {} "
          "| Samples: {}".format(id, datetime, payload, msg_type, msg_stype, samples))

    msg_type_str = MessageTypeToString(msg_type, msg_stype)
    print(msg_type_str)

    if msg_type_str is None:
        return -1

    for s in samples:
        timestamp = GenerateTimestamp(msg_type, msg_stype, datetime, len(samples))
        query = MessageDataToQuery(s, id, msg_type_str, timestamp)
        print(client.execute(query))

    gen = MessageTypeMap[msg_stype]["timestamp_gen"]["B"]
    gen.Reset()
    gen.SavePreviousReceiveTimestamp()

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
