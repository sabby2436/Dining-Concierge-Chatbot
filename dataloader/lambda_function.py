import yelpapi
from yelpapi import YelpAPI
import json
import boto3
from decimal import Decimal
import requests
import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("yelp_restaurants_1")

region = 'us-east-1'
service = 'es'

host = 'https://search-restaurants-kahefunwonk5zpz3i6q737vrnu.us-east-1.es.amazonaws.com'

index = 'restaurants'
type = 'Restaurant'

url = host + '/' + index + '/' + type + '/'

headers = { "Content-Type": "application/json" }

# write private api_key to access yelp here
api_key = 'wCUL6ldYDFCLdS6l8rlouRdVHkSUxdroA9K0vTjxM6hElRtDHWaZBGIz-hTNqQ_HJkUb94Rux-ISpNoQmG7_XuWqI0Ah7RnIrHFKU8QuE68J_NKKweRODQX1U72EX3Yx'

yelp_api = YelpAPI(api_key)

data = ['id', 'name', 'review_count', 'rating', 'coordinates', 'address1', 'zip_code', 'phone']
es_data = ['id']

# cuisines = ["thai", "chinese", "mexican"]
cuisines = ["chinese", "indian", "mexican", "american", "italian"]


def populate_database(response, cuisine):
    json_response = json.loads(json.dumps(response), parse_float=Decimal)
    for t in json_response["businesses"]:
        dict1 = { key:value for (key,value) in t.items() if key in data}
        dict2 = {key:value for (key,value) in t["location"].items() if key in data}
        dict1.update(dict2)
        dict1.update(cuisine=cuisine)
        final_dict = {key: value for key, value in dict1.items() if value}
        timeStamp = str(datetime.datetime.now())
        final_dict.update(insertedAtTimestamp=timeStamp)
        
        my_es_id  = final_dict['id']
        es_dict = {key: final_dict[key] for key in final_dict.keys() & {'id', 'cuisine'}} 
        docs = json.loads(json.dumps(es_dict))
        
        r = requests.put(url+str(my_es_id), json=docs, headers=headers)
        print(r)
        table.put_item(Item=final_dict)
        

def lambda_handler(event=None, context=None):
    for cuisine in cuisines:
        for x in range(0, 1000, 50):
            response = yelp_api.search_query(term=cuisine, location='Manhattan', limit=50, offset=x)
            populate_database(response, cuisine)
