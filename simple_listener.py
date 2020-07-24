import tweepy
from storage import Storage
import time

'''
    my class which organize the tweepy library for the streaming API
    and to store the tweets from streaming API to the storage
'''
class SimpleListener(tweepy.StreamListener):
    def __init__(self, _storage, time_limit=3600):
        super().__init__()
        self.start_time = time.time()
        # 3600 seconds = 1 hour
        self.limit = time_limit 
        self.storage = _storage

    def on_status(self, status): 
        # code to run each time the stream receives a status
        print(status)
    
    def on_data(self, data):
        # code to run each time you receive some data (direct message, delete, profile update, status,...)
        # set the time limit until when the streamer should be run, when initializing
        if (time.time() - self.start_time) < self.limit:
            self.storage.save_str_data(data)
            return True
        else:
            return False
        
    def on_error(self, status_code):
        # code to run each time an error is received
        if status_code == 420:
            return False
        else:
            return True