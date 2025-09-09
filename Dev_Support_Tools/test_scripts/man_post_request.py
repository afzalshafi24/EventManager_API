

import requests
import sys
import datetime

'''
Sample Script Call:
    python man_post_request.py 10 PARS_Divergence ECQL http://ITE00647722:8000
JSON Fields:
    scid: int
    event_time: str
    event_rule_id: int
    event_name: str
    event_src: str
    spark_script: Optional[str]
    spark_report_id: Optional[int]
    gem_full_path: Optional[str]
'''


def send_POST_request(scid, event_name, event_src, uri):
    # Get the current date and time
    now = datetime.datetime.now()

    # Format the date and time
    event_time = now.strftime("%Y-%m-%d %H:%M:%S")
    
    response = requests.post(f"{uri}/event/submit", 
                                json={'scid': scid,
                                      'event_time': event_time,  
                                      'event_rule_id': 1,
                                      'event_name': event_name,
                                      'event_rule': '3 > threshold',
                                      'event_src': event_src,
                                      'spark_script': 'GMP',
                                      'spark_report_id': 1,
                                      'gem_full_path': r'C:/vcid.pgem'})
                
    

if __name__ == '__main__':
    scid = int(sys.argv[1])
    event_name = sys.argv[2]
    event_src = sys.argv[3]
    uri = sys.argv[4]

    send_POST_request(scid, event_name, event_src, uri)