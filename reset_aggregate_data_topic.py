#!/usr/bin/env python

from confluent_kafka import Consumer
import json
# import ccloud_lib
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

    consumer_id = input('Please input the consumer group id for which you want to clear messages on the aggregate_data topic on Kafka server -> ')

    conf = {
                'bootstrap.servers':'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
                'security.protocol':'SASL_SSL',
                'sasl.mechanisms':'PLAIN',
                'sasl.username':'REVSTSDUYSLPYRQH',
                'sasl.password':'yf0PPm5bSDCC+fyj6mDDky7di3gg7uOIiq0a4I9RANbQLqIBWa195OsJ/XhvkTx7',


                'group.id':consumer_id,
                'auto.offset.reset':'earliest',
    }

    # consumer_raw = Consumer(conf)
    consumer_aggregate = Consumer(conf)
    # raw_data_topic = 'raw_data'
    aggregate_data_topic = 'aggregate_data'

    # Subscribe to topic
    consumer_aggregate.subscribe([aggregate_data_topic])

    # Process messages
    total_count = 0
    try:
        while True:
            msg = consumer_aggregate.poll(1.0)
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
                # count = data['count']
                # total_count += count
                print("Consumed record with key {} and value \n"
                      .format(record_key))

                # networks_df = pd.DataFrame(data)
                print(record_value, '\n\n')
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer_aggregate.close()