

import requests
import sys
import datetime

'''-scid 10 -metric_name 'GMP' -source 'ECQL' -uri '127.0.0.1:8000/ECQL_store
'''

def send_POST_request(scid, metric_name, source, uri):
    # Get the current date and time
    now = datetime.datetime.now()

    # Format the date and time
    t = now.strftime("%Y-%m-%d %H:%M:%S")
    
    if source == "ECQL":
        response = requests.post(uri, json={'scid': scid, 't': t,  'metric_name': metric_name, 'source': source})
    elif source == "ASTRA":
        print(source)
        response = requests.post(uri, json={'scid': scid,
                                            'start_time': t,
                                            'duration': 100,
                                            't': t,  
                                            'metric_name': metric_name, 
                                            'value' : 0,
                                            'persistence_flag' : 'T',
                                            'num_persistence_samples' : 30,
                                            'on_change_flag' : 'F',
                                            'threshold_value': 78.2,
                                            'plotLink' : 'WHO KNOWS',
                                            'description': 'I KNOW',
                                            'source': source})


if __name__ == '__main__':
    scid = int(sys.argv[1])
    metric_name = sys.argv[2]
    source = sys.argv[3]
    uri = sys.argv[4]

    send_POST_request(scid, metric_name, source, uri)