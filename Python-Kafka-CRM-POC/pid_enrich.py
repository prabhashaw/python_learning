from pymongo import MongoClient
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json

pid_enrich_topic = 'PID_Enrichment_topic'
prev_topic = 'iidr_trans'


class PID_Enrichment:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        pid_enrich = client.pid_enrich
        self.s_doc_quote = pid_enrich.s_doc_quote
        self.s_quote_tntx = pid_enrich.s_quote_tntx
        x = self.s_quote_tntx.find_one()
        # print(x)

    def consume_msg(self, prev_topic):
        # a simple json message is sent on topic (contains PAR_ROW_ID) is consumed here.
        consumer = KafkaConsumer(prev_topic, bootstrap_servers='localhost:9092')
        return consumer

    def find_valueOf_row_id(self, json_msg):
        # fetch the value of par_raw_id from the consumer record
        par_row_id = json_msg["PAR_ROW_ID"]
        print("par_row_id : ", par_row_id)
        return par_row_id

    def get_property_id(self, par_row_id):
        # Here the row_id's value is same as par_raw_id.Get the value of BU_ID from each record of row_id.
        # Assign the value of BU_ID to property id.
        if par_row_id != '':
            dup_par_id = self.s_doc_quote.find_one({"ROW_ID": par_row_id})
            print("dup_par_id  :--->", dup_par_id)
            one_row = dup_par_id["data"]
            jmsg_from_table = json.loads(one_row)
            prop_id = jmsg_from_table["BU_ID"]
            print(prop_id)
            return prop_id

    def pid_enrichment(self, json_msg, prop_id):
        # assign value of bu_id into 'ODS_PROPERTY_ID' ('S_QUOTE_TNTX' collection).
        json_msg["ODS_PROPERTY_ID"] = prop_id
        header = [('Table.Key', b'S_QUOTE_TNTX'), ('Flow.Type', b'PID_Enrichment')]
        enrich_message = json.dumps(json_msg)
        return enrich_message, header

    def pid_producer(self, enrich_message, header):
        # it will produce the entire  collection (json file enriched with 'ODS_PROPERTY_ID' field into the topic)
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        print(" producer_record ", enrich_message, "Header = ", header)
        producer.send(pid_enrich_topic, value=enrich_message.encode(), headers=header)
        producer.flush()


def main():
    while True:
        pid = PID_Enrichment()
        consumer = pid.consume_msg(prev_topic)
        for msg in consumer:
            print('msg :', msg)
            json_msg = json.loads(msg.value)
            print("json_msg :", json_msg)
            par_row_id = pid.find_valueOf_row_id(json_msg)
            prop_id = pid.get_property_id(par_row_id)
            enrich_message, header = pid.pid_enrichment(json_msg, prop_id)
            pid.pid_producer(enrich_message, header)


if __name__ == "__main__":
    main()
