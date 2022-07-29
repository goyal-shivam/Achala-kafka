#!/usr/bin/env python

from threading import currentThread
from confluent_kafka import Producer, Consumer
import json
import pandas as pd
import time 


if __name__ == '__main__':


    delivered_records = 0
    round_time = 2
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
        # 'auto.offset.reset=earliest' to start reading from the beginning of the
        #   topic if no committed offsets exist
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
    round_number = 0
    try:
        while True:

            startTime = currentTime = time.time()
            tablesList = []
            is_waiting = False
            while(currentTime - startTime < round_time):
                msg = consumer.poll(1.0)
                currentTime = time.time()
                if msg is None:
                    # No message available within timeout.
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    if(not is_waiting):
                        print("Waiting for message or event/error in poll()")
                        is_waiting = True
                    continue
                elif msg.error():
                    print('error: {}'.format(msg.error()))
                    is_waiting = False
                else:
                    # Check for Kafka message
                    record_key = msg.key()
                    record_value = msg.value()
                    data_dict = json.loads(record_value)
                    print('----------------------------------------------------------------------')
                    print(f"Consumed record with key - {record_key}", end='')

                    is_waiting = False

                    networks_df = pd.DataFrame(data_dict)
                    
                    # Adding to TablesList which will later be sent to CR (Conflict Resolution) module 
                    tablesList.append(networks_df)
                    print(f'\tTable length = {len(tablesList)}')
            # data = Conflict_Resolution_Algorithm(tablesList)
            record_key = 'aggregated_data'
            round_number += 1
            print(f"\nTable List length is {len(tablesList)}, round number = {round_number}. The table is as follows: ")

            for i in range(len(tablesList)):
                print(f'\n{i+1}.\n{tablesList[i]}')

            print()

            tablesList = []

            data = {
                'COL1': {
                    '0': 'ROW1COL1',
                    '1': 'ROW2COL1'
                },
                'COL2': {
                    '0': 'ROW1COL2',
                    '1': 'ROW2COL2'
                }
            }
            
            producer.produce(
                aggregate_data_topic,
                key=record_key,
                value=json.dumps(data, indent=4),
                on_delivery=acked
            )

            producer.flush()

            # p.poll() serves delivery reports (on_delivery)
            # from previous produce() calls.
            producer.poll(0)

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
