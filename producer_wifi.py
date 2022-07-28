# !/usr/bin/env python

from confluent_kafka import Producer, Consumer
import json
from sys import platform
import subprocess
import pandas as pd
from time import sleep, time

if __name__ == '__main__':

    # # Read arguments and configurations and initialize
    # args = ccloud_lib.parse_args()
    # config_file = args.config_file
    # # topic = args.topic
    # conf = ccloud_lib.read_ccloud_config(config_file)
    raw_data_topic = 'raw_data'
    aggregate_data_topic = 'aggregate_data'
    producer_id = input('Please enter a unique producer id -> ')

    # # Create Producer instance
    # producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)

    producer_conf = {
                'bootstrap.servers':'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
                'security.protocol':'SASL_SSL',
                'sasl.mechanisms':'PLAIN',
                'sasl.username':'REVSTSDUYSLPYRQH',
                'sasl.password':'yf0PPm5bSDCC+fyj6mDDky7di3gg7uOIiq0a4I9RANbQLqIBWa195OsJ/XhvkTx7'
            }
    consumer_conf = {
                'bootstrap.servers':'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
                'security.protocol':'SASL_SSL',
                'sasl.mechanisms':'PLAIN',
                'sasl.username':'REVSTSDUYSLPYRQH',
                'sasl.password':'yf0PPm5bSDCC+fyj6mDDky7di3gg7uOIiq0a4I9RANbQLqIBWa195OsJ/XhvkTx7',


                'group.id':'python_example_group_1',
                'auto.offset.reset':'earliest',
    }
    producer = Producer(producer_conf)
    consumer = Consumer(consumer_conf)
    consumer.subscribe([aggregate_data_topic])

    # Create topic if needed
    # ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
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

    def producer_send(producer, networks_df, topic=raw_data_topic):
        networks_df['producer_id'] = producer_id
        networks_df['timestamp'] = time()
        print('----------------------------------------------------------------------')
        print("pandas dataframe\n", networks_df)
        data = networks_df.to_json()

        record_key = "data"
        record_value = data

        print("sending the data and sleeping for 10 sec-----------------------------------------")

        producer.produce(
            topic,
            key=record_key,
            value=record_value,
            on_delivery=acked
        )

        producer.flush()

        print("{} messages were produced to topic {}!".format(delivered_records, topic))
        sleep(10)

        
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)
        
        #-----------------------------------------------
        # Here we are receiving the aggregated table:
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

                    print(data, '\n\n')
                    break
        except KeyboardInterrupt:
            pass
        

    print("Platform is: ", platform)


    if platform == 'linux' or platform == 'linux2':
        while True: 
            # subprocess.check_output('nmcli dev wifi rescan', shell=True)
            output = subprocess.check_output(['nmcli', '-f', 'BSSID,SSID','dev' ,'wifi'])
            output = output.decode('utf-8')
            output= output.replace("\r","")

            networks_df = pd.DataFrame([x.split(' ', 1) for x in output.split('\n')[1:-1]])
            networks_df.columns = ['BSSID', 'SSID']
            
            
            
            
            
            producer_send(producer, networks_df=networks_df, topic=raw_data_topic)

    elif platform == 'darwin':
        # OS X
        while True: 
            scan_cmd = subprocess.Popen(['sudo', '/System/Library/PrivateFrameworks/Apple80211.framework/Versions/Current/Resources/airport', '-s'],    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            scan_out, scan_err = scan_cmd.communicate()
            scan_out = scan_out.decode('utf-8')
            # print(scan_out)
            
            networks_df = pd.DataFrame(columns = ['BSSID', 'SSID'])

            # Below splitting is based on print format of airport command
            # bssid_index = scan_out.find("BSSID")
            # rssi_index = scan_out.find("RSSI")
            # for line in scan_out.split('\n')[1:-1]:
            #     ssid = line[:bssid_index - 1].strip()
            #     bssid = line[bssid_index:rssi_index - 1]
            #     networks_df = pd.concat([networks_df, pd.DataFrame({'BSSID': bssid, 'SSID': ssid}, index=[0])]).reset_index(drop = True)
            for line in scan_out.split('\n')[1:-1]:
                last_colon_index = line.rfind(':')
                bssid_index = last_colon_index - 14
                rssi_index = last_colon_index + 4
                ssid = line[:bssid_index - 1].strip()
                bssid = line[bssid_index:rssi_index - 1]
                networks_df = pd.concat([networks_df, pd.DataFrame({'BSSID': bssid, 'SSID': ssid}, index=[0])]).reset_index(drop = True)
            producer_send(producer, networks_df=networks_df, topic=raw_data_topic)

    elif platform == 'win32':
        while True: 
            # using the check_output() for having the network term retrieval
            devices = subprocess.check_output(['netsh','wlan','show','network', 'bssid'])

            # print(devices)
            # decode it to strings

            devices = devices.decode('utf-8')
            devices= devices.replace("\r","")
            
            # displaying the information
            # print(devices)
            networks_df = pd.DataFrame(columns = ['BSSID', 'SSID'])
            ssid=""
            bssid=""
            # code snippet below adds the bssid and ssid to pandas dataframe.
            for line in devices.splitlines():
                if(line.strip().startswith('SSID')):
                    sindex = line.find(': ')
                    ssid = line[sindex +2:]
                    ssid.strip()
                    # print(ssid)
                if(line.strip().startswith('BSSID')):
                    sindex = line.find(': ')
                    bssid = line[sindex +2:]
                    bssid.strip()
                    # print(bssid)

                if(len(ssid) and len(bssid)):
                    networks_df = pd.concat([networks_df, pd.DataFrame({'BSSID': bssid, 'SSID': ssid}, index=[0])]).reset_index(drop = True)
                    ssid = bssid = ""
            producer_send(producer, networks_df=networks_df, topic=raw_data_topic)







# handle partitioning of the aggregated table that comes back as a response
# allow multiple consumers
# remove pprint library


# json_dict = json.loads(json_dict)
# record_value = json.dumps(json_dict, indent=4)
# remove this contradicting opposite code


# simplify sending json, then converting into dataframe just for printing,
# do something to print in a nice format, and remove redundancies
