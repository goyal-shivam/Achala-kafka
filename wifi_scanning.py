# importing the subprocess module
from stat import S_ISDIR
from posixpath import split
from sys import platform
import subprocess
import pandas as pd
from time import sleep



# Unix commands
# nmcli dev wifi
# nmcli -f BSSID,SSID dev wifi  #prints strings
# nmcli -g BSSID,SSID dev wifi  #prints backslash separated
print("Platform is: ", platform)


if platform == 'linux' or platform == 'linux2':
    while True: 
        # subprocess.check_output('nmcli dev wifi rescan', shell=True)
        output = subprocess.check_output(['nmcli', '-f', 'BSSID,SSID','dev' ,'wifi'])
        output = output.decode('utf-8')
        output= output.replace("\r","")
        # print(output)
        networks_df = pd.DataFrame(columns = ['BSSID', 'SSID'])
        
        for line in output.splitlines()[1:]:
            details = line.split(' ', 1)
            bssid = details[0].strip()
            ssid = details[1].strip()
            networks_df = pd.concat([networks_df, pd.DataFrame({'BSSID': bssid, 'SSID': ssid}, index=[0])]).reset_index(drop = True)
        # networks_df = pd.DataFrame([x.split(' ', 1) for x in output.split('\n')[1:-1]])
        # networks_df.columns = ['BSSID', 'SSID']
        print('----------------------------------------------------------------------')
        print("pandas dataframe\n", networks_df)
        sleep(5)

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
        print('----------------------------------------------------------------------')
        print("pandas dataframe\n", networks_df)
        sleep(5)

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
                # networks_df = networks_df.append({'BSSID': bssid, 'SSID': ssid}, ignore_index=True)
                networks_df = pd.concat([networks_df, pd.DataFrame({'BSSID': bssid, 'SSID': ssid}, index=[0])]).reset_index(drop = True)
                ssid = bssid = ""
        print('----------------------------------------------------------------------')
        print("pandas dataframe\n", networks_df)
        sleep(5)
