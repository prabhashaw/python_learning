from pymongo import MongoClient
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json

pid_enrich_topic = 'PID_Enrichment_topic'
prev_topic = 'iidr_trans'

class ODS_Insert:

    def __init__(self):
        client = MongoClient('localhost', 27017)
        ods_insert = client.ods_insert
        self.pid_enrich_collection = ods_insert.pid_enrich_collection
        # x = self.s_quote_tntx.find_one()
        # # print(x)

    def consume_msg(self, pid_enrich_topic):
        # a PID_Enrich json message is sent on pid_enrich_topic  is consumed here.
        consumer = KafkaConsumer(pid_enrich_topic, bootstrap_servers='localhost:9092')
        return consumer

    def insert_json(self, json_msg):
        # insert each consumer record into table
        self.pid_enrich_collection.insert_one(json_msg)
        print("inserted into table: ", json_msg)

def main():
    while True:

        odsIn = ODS_Insert()
        consumer = odsIn.consume_msg(pid_enrich_topic)
        for msg in consumer:
            json_msg = json.loads(msg.value)
            odsIn.insert_json(json_msg)


if __name__ == "__main__":
    main()
