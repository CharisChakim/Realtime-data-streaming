# from airflow import DAG
from datetime import datetime
# from airflow.operator.python import PythonOperator
# import json

def get_data():
    import requests
    
    respone = requests.get("https://randomuser.me/api/")
    res = respone.json()
    res = res['results'][0]
    return res
# print(get_data())

def format_data(res):
    data = {}
    location = res ['location']
    data['first_name'] = res ['name']['first']
    data['last_name'] = res ['name']['last']
    data['gender'] = res ['gender']
    data['addres'] = f"{str(location['street']['number'])} {location['street']['name']}" \
                     f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res ['email']
    data['username'] = res ['login']['username']
    data['dob'] = res ['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res ['phone']
    data['picture'] = res['picture']['medium']
    return data
    
def stream_data():
    import json
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3))
# stream_data()


# default_args={
#     'owner'     :'charis'
#     'start_date': datetime(2024,1,1,10,00)
# }

# with DAG(
#     'user_automation',
#     default_args =default_args,
#     schedule_interval = '@daily',
#     cathup = False
# ) as dag :
    
#     streaming_task= PythonOperator(
#         task_id='stream_data_from_api'
#         python_callable=stream_data
#     )