import random
import sys
import six
from datetime import datetime
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import radar

class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, source_symbol):
        price_field = random.randint(800,1400)
        msg_cnt = 0
	category_product = [('furniture','cat bed'), ('dog food','purina dog biscuits'),('cat food','fancy feast 8oz'),('cleaning','roomba')]
        
	while True:

            rand_datetime = radar.random_datetime(start = datetime(year=2016, month=5, day=24),stop = datetime(year=2017, month=2, day=1))
            time_field = rand_datetime.strftime("%Y%m%d %H%M%S")
            price_field += random.randint(-10, 10)/10.0
            product_cat_listid = random.randint(0, 2)
	    customer_field = random.randint(1,10000)
	    product_field = category_product[product_cat_listid][1]
	    category_field = category_product[product_cat_listid][0]
            volume_field = random.randint(1,10)
	    str_fmt = "{};{};{};{};{};{};{}"
            message_info = str_fmt.format(source_symbol,
                                          time_field,
                                          price_field,
                                          volume_field,
					  customer_field,
					  product_field,
					  category_field)
            print message_info
            self.producer.send_messages('transactiondata', source_symbol, message_info)
            msg_cnt += 1

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)
