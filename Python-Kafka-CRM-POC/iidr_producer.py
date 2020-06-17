import os
# from kafka import KafkaProducer

class iidrProducer:
    producer_topic = 'iidr_trans'
    valid_table_key = ['s_doc_quote', 's_quote_tntx']

    def produceMsg(self, file_pth, tbl_key):
        from kafka import KafkaProducer
        hdr = [('Table.Key', tbl_key.encode())]
        print("Header:", hdr)
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        file = open(file_pth, "r")
        count = 0
        for line in file:
            count += 1
            msg_val = line.strip("\n")
            print("Msg_value:", msg_val)
            producer.send(self.producer_topic, value=msg_val.encode(), headers=hdr)
            producer.flush()
        file.close()
        print("{0} messages sent successfully:".format(count))

    def getFileName(self, filepath):
        filename1, file_ext = os.path.splitext(filepath)
        print(filename1, file_ext)
        slsh_pos = filepath.rindex("\\")
        filename = filepath[slsh_pos + 1:]
        print (filename)
        table_key = 'prabha' #filename[:filename.rindex('.')]
        # if table_key in self.valid_table_key:
        #     print(table_key)
        # else:
        #     print("Enter valid file name: ")
        #     exit()
        # print(filepath)
        return (table_key, filepath)
    

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    else:
        print("Note: Please enter Filepath Parameter!!")
        exit()

    ip = iidrProducer()
    table_key, file_path = ip.getFileName(filepath)
    ip.produceMsg(file_path, table_key)