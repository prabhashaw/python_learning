# -*- coding: utf-8 -*-
import mysql.connector
import scrapy
from mysql.connector import errorcode
import datetime

class MysqlPipeline():
    cnx = mysql.connector.connect(user='prabha', password='prabha123',
                                  host='127.0.0.1',
                                  database='web_scraping')
    print("connection good")
    cursor = cnx.cursor()
    add_quote = ("INSERT INTO web_scraping.quotes"
                 "(quote, author, tags, last_upd_timestamp) "
                 "VALUES (%s, %s, %s, %s)")

    def __init__(self):
        print(self.cursor.items())

    def process_item(self, item, spider):

        tags = self.list_to_string(item['tags'])
        try:
            self.cursor.execute(self.add_quote, (item['text'].encode('utf-8'),
                                                 item['author'].encode('utf-8'),
                                                 tags.encode('utf-8'),
                                                 self.get_time_now()))
            self.cnx.commit()
            print("\ninserted--->", item)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        return item

    def list_to_string(self, s):
        # initialize an empty string
        str1 = ""
        # traverse in the string
        for ele in s:
            str1 += ele
            # return string
        return str1

    def get_time_now(self):
        now = datetime.datetime.utcnow()  # now()
        today_now = now.strftime("%Y-%m-%d %H:%M:%S")
        return today_now
