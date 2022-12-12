from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import credentials
from pykafka import KafkaClient #파이카프카
from datetime import datetime
import json #json import
import sys
import numpy as np

#내 로컬 IP에 클라이언트 만들기
def get_kafka_client():
    return KafkaClient(hosts='localhost:9092')


class StdOutListener(StreamListener):
    def on_data(self, data):
        print(data)
        message = json.loads(data)
        result_json = dict()

        try:
            lat = message['place']['bounding_box']['coordinates'][0][0][0]
            long = message['place']['bounding_box']['coordinates'][0][0][1]

        except:
            lat = np.nan
            long = np.nan
        try:
            if ('RT' in message['text']) and ('retweeted_status' in message.keys()):  # 리트윗
                status = 'RT'
                post_id = message['retweeted_status']['id']
                screen_name = message['retweeted_status']['user']['screen_name']
                user_id = message['retweeted_status']['user']['id']
                profile = message['retweeted_status']['user']['profile_image_url']
                time = datetime.strftime(datetime.strptime(message['retweeted_status']['user']['created_at'],'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S')

                try:
                    text = message['retweeted_status']['extended_tweet']['full_text']
                except:
                    text = message['retweeted_status']['text']
                retweets = message['retweeted_status']['retweet_count']
                likes = message['retweeted_status']['favorite_count']

            elif 'quoted_status' in message.keys():
                status = 'QUOTED'
                post_id = message['quoted_status']['id']
                screen_name = message['quoted_status']['user']['screen_name']
                user_id = message['quoted_status']['user']['id']
                profile = message['quoted_status']['user']['profile_image_url']
                time = datetime.strftime(datetime.strptime(message['quoted_status']['user']['created_at'],'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S')

                try:
                    text = message['quoted_status']['extended_tweet']['full_text']
                except:
                    text = message['quoted_status']['text']
                retweets = message['quoted_status']['retweet_count']
                likes = message['quoted_status']['favorite_count']

            else:
                status = 'ORIGINAL'
                post_id = message['id']
                screen_name = message['user']['screen_name']
                user_id = message['user']['id']
                profile = message['user']['profile_image_url']
                time = datetime.strftime(datetime.strptime(message['created_at'],'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S')
                text = message['text']
                retweets = message['retweet_count']
                likes = message['favorite_count']
        except:
            print("err")
            status = 'ERR'
            pass

        result_json['status'] = status
        result_json['id'] = post_id
        result_json['screen_name'] = screen_name
        result_json['user_id'] = user_id
        result_json['profile'] = profile
        result_json['time'] = time
        result_json['text'] = text
        result_json['retweet_count'] = retweets
        result_json['favorite_count'] = likes
        # result_json['lat'] = lat
        # result_json['long'] = long
        result_json['group'] = 'bts'

        if result_json['status'] is not 'ERR':
            client = get_kafka_client()
            topic = client.topics['groups']
            producer = topic.get_sync_producer() # RUN THE PRODUCER
            print(result_json)
            result_encode = json.dumps(result_json).encode('utf-8')
            producer.produce(result_encode) #프로듀서가 실행되면 만들수있음. 카프카는 바이트 데이터로만 됨
            
            #sys.exit(0)
            return True

    def on_error(self, status):
        print(status)

if __name__ == "__main__":
    auth = OAuthHandler(credentials.API_KEY, credentials.API_SECRET_KEY)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
    listener = StdOutListener()
    stream = Stream(auth, listener)
    # stream.filter(track=['장마','홍수', '범람', '침수'])
    stream.filter(track=['bts'])
    #stream.filter(locations=[-180,-90,180,90])
