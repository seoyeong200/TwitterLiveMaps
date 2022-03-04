from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import twitter_credentials
import json
from google.cloud import pubsub_v1
from google.oauth2 import service_account

key_path = 'tweetdeck-320105-9820690ac16e.json'

credentials = service_account.Credentials.from_service_account_file(
    key_path, 
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
)

client = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = client.topic_path('tweetdeck-320105', 'twitterdata')


class StdOutListener(StreamListener):
    def __init__(self,keyword_list):
        self.keyword_list = keyword_list

    def on_data(self,data):
        data = json.loads(data)
        # keyword_list = "_".join(self.keyword_list)
        # keyword_list = str(keyword_list.strip())
        data['keyword_list'] = str("코로나")
        print(data)
        message = json.dumps(data)
        client.publish(topic_path, data=message.encode('utf-8'))
        return True
    
    def on_error(self, status):
        print(status)

def sendTweet(keyword_list):
    auth = OAuthHandler(twitter_credentials.API_KEY, twitter_credentials.API_SECRET_KEY)
    auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
    listener = StdOutListener(keyword_list)
    stream = Stream(auth, listener)
    stream.filter(track=keyword_list)

if __name__ == "__main__":
    keyword_list = ['코로나']
    sendTweet(keyword_list)