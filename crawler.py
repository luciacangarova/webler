import tweepy
import json
import time
from storage import Storage
from simple_listener import SimpleListener

'''
    my class which organize the tweepy library to run the streaming API and REST API
    and to store the tweets from REST API to the storage
'''
class Crawler():
    def __init__(self, _access_token, _access_secret, _consumer_key, _consumer_secret, _storage, words_to_track, users_to_track, time_limit):
        self.storage = _storage
        self.api = ''
        self.streamer = ''
        # words to track
        self.words = words_to_track
        # users to track
        self.users = users_to_track
        # credentials
        self.access_token = _access_token
        self.access_secret = _access_secret
        self.consumer_key = _consumer_key
        self.consumer_secret = _consumer_secret
        # time limit
        self.time_limit = time_limit
        # create REST API and streaming API
        self.create_API()
        self.create_streamer()

    def create_API(self):
        # OAuth process, using the keys and tokens
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_secret)
        # create rest API
        self.api = tweepy.API(auth)

    def create_streamer(self):
        # OAuth process, using the keys and tokens
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_secret)
        # create listener and streamer with appropriate limit
        listener = SimpleListener(self.storage, self.time_limit)
        self.streamer = tweepy.Stream(auth=auth, listener=listener, tweet_mode='extended')

    def run_streamer(self):
        # run the streaming API using filter function on the words given to the class
        self.streamer.filter(track=self.words, languages=['en'])

    def run_rest_search(self):
        # run the REST api using search function to track the words given to the class
        for i in range(len(self.words)):
            for status in tweepy.Cursor(self.api.search, lang="en", include_entities=True, q=self.words[i], tweet_mode='extended').items(1000):
                # save data to the MongoDB
                self.storage.save_obj_data(status)
        
    def run_rest_user_timeline(self):
        # run the REST api using user_timeline function to track the usernames given to the class
        for i in range(len(self.users)):
            for status in tweepy.Cursor(self.api.user_timeline, screen_name=self.users[i], lang="en", include_rts=False, tweet_mode='extended').items(1000):
                # save data to the MongoDB
                self.storage.save_obj_data(status)