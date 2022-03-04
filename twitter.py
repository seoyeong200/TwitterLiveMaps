from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import credentials
from pykafka import KafkaClient
# from kafka import KafkaProducer
import json

def get_kafka_client():
    return KafkaClient(hosts='127.0.0.1:9092')

class StdOutListener(StreamListener):
    def on_data(self, data):
        print(data)
        message = json.loads(data)
        
        # if message['place'] is not None:
        client = get_kafka_client()
        # 트위터 데이터를 produce할 토픽 이름 지정
        topic = client.topics['twitterdata']
        producer = topic.get_sync_producer()
        producer.produce(data.encode('ascii'))
        return True

    def on_error(self, status):
        print(status)
        if status == 420:
            #returning False in on_data disconnects the stream
            return False
        

if __name__ == "__main__":
    auth = OAuthHandler(credentials.API_KEY, credentials.API_SECRET_KEY)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
    listener = StdOutListener()
    stream = Stream(auth, listener)
    stream.filter(track=['더위', '더워', '날씨', '여름'])
    # stream.filter(locations=[-180,-90,180,90])


# Authorization and Authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

if __name__ == "__main__":
    # Available Locations
    available_loc = api.trends_available()
    # writing a JSON file that has the available trends around the world
    with open("available_locs_for_trend.json","w") as wp:
        wp.write(json.dumps(available_loc, indent=1))

    # Trends for Specific Country
    loc = sys.argv[1]     # location as argument variable 
    g = geocoder.osm(loc) # getting object that has location's latitude and longitude

    closest_loc = api.trends_closest(g.lat, g.lng)
    trends = api.trends_place(closest_loc[0]['woeid'])
    # writing a JSON file that has the latest trends for that location
    with open("twitter_{}_trend.json".format(loc),"w") as wp:
        wp.write(json.dumps(trends, indent=1))