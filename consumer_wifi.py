#!/usr/bin/env python

from confluent_kafka import Producer, Consumer
import json
from pprint import pprint
import pandas as pd


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    # args = ccloud_lib.parse_args()
    # config_file = args.config_file
    # topic = args.topic
    # conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    # consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    # consumer_conf['group.id'] = 'python_example_group_1'
    # consumer_conf['auto.offset.reset'] = 'earliest'
    # consumer = Consumer(consumer_conf)

    delivered_records = 0

    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))


    consumer_conf = {
                'bootstrap.servers':'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
                'security.protocol':'SASL_SSL',
                'sasl.mechanisms':'PLAIN',
                'sasl.username':'REVSTSDUYSLPYRQH',
                'sasl.password':'yf0PPm5bSDCC+fyj6mDDky7di3gg7uOIiq0a4I9RANbQLqIBWa195OsJ/XhvkTx7',


                'group.id':'python_example_group_1',
                'auto.offset.reset':'earliest',
    }

    consumer = Consumer(consumer_conf)

    producer_conf = {
                'bootstrap.servers':'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
                'security.protocol':'SASL_SSL',
                'sasl.mechanisms':'PLAIN',
                'sasl.username':'REVSTSDUYSLPYRQH',
                'sasl.password':'yf0PPm5bSDCC+fyj6mDDky7di3gg7uOIiq0a4I9RANbQLqIBWa195OsJ/XhvkTx7'
            }

    producer = Producer(producer_conf)

    raw_data_topic = 'raw_data'
    aggregate_data_topic = 'aggregate_data'

    # Subscribe to topic
    consumer.subscribe([raw_data_topic])

    # Process messages
    total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                print("Consumed record with key {} and value \n"
                      .format(record_key))

                # pprint(data)
                networks_df = pd.DataFrame(data)
                print(networks_df, '\n\n')
                # write a condition here when aggregated table should be sent, after every 5 seconds, or after all the mobiles have sent their raw tables

                record_key = 'data'
                data = {
                    'data1' : 'answer1',
                    'data2' : 'answer2'
                }
                record_value = json.dumps(data, indent=4)
                print("Producing record: {} and value \n{}".format(record_key, data))

                producer.produce(
                    aggregate_data_topic,
                    key=record_key,
                    value=record_value,
                    on_delivery=acked
                )

                producer.flush()

                print("{} messages were produced to topic {}!".format(delivered_records, aggregate_data_topic))

                
                # p.poll() serves delivery reports (on_delivery)
                # from previous produce() calls.
                producer.poll(0)

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
