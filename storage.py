import pymongo
import json
import datetime


'''
    my class which organize the mongoDB library to store the tweets
'''
class Storage():

    def __init__(self, host_name):
        # create the client of MongoDB with appropriate host name
        self.client = pymongo.MongoClient(host_name)
        # create database
        self.db = self.client.mydatabase
        # create collections to hold tweets and log data (outcome data)
        self.collection = self.db.collected_tweets
        self.log_collection = self.db.log
        # delete all tweets and logs in my database if there are any
        self.collection.delete_many({})
        self.log_collection.delete_many({})
        # initialize variables to count duplicates and tweets taken from streaming and REST API
        self.duplicates = 0
        self.data_from_stream = 0
        self.data_from_rest = 0

    # save data from streaming API
    def save_str_data(self, data):
        try:    
            # Decode the JSON from Twitter
            data_json = json.loads(data)

            # if it is retweet, tweet does not have full text
            if 'retweeted_status' in data_json:
                is_retweet = True
                if data_json['retweeted_status']['truncated']:
                    text = data_json['text'].split(':')[0] + ': ' + data_json['retweeted_status']['extended_tweet']['full_text']
                    hashtags = data_json['retweeted_status']['extended_tweet']['entities']['hashtags']  # Any hashtags used in the Tweet
                    mentions = data_json['retweeted_status']['extended_tweet']['entities']['user_mentions'] # any mention used in the tweet
                else:
                    text = data_json['text'].split(':')[0] + ': ' + data_json['retweeted_status']['text']
                    hashtags = data_json['retweeted_status']['entities']['hashtags']  # Any hashtags used in the Tweet
                    mentions = data_json['retweeted_status']['entities']['user_mentions'] # any mention used in the tweet
            else:
                is_retweet = False

                # if tweet is truncated
                truncated = data_json['truncated']

                if truncated:
                    text = data_json['extended_tweet']['full_text']
                    hashtags = data_json['extended_tweet']['entities']['hashtags']  # Any hashtags used in the Tweet
                    mentions = data_json['extended_tweet']['entities']['user_mentions'] # any mention used in the tweet
                else:
                    text = data_json['text']  # The entire body of the Tweet
                    hashtags = data_json['entities']['hashtags']  # Any hashtags used in the Tweet
                    mentions = data_json['entities']['user_mentions'] # any mention used in the tweet
                    


            # Pull important data from the tweet to store in the database.
            tweet_id = data_json['id_str']  # The Tweet ID from Twitter in string format
            username = data_json['user']['screen_name']  # The username of the Tweet author
            followers = data_json['user']['followers_count']  # The number of followers the Tweet author has
            tweets_number = data_json['user']['statuses_count'] # the number of tweets and retweets of user
            dt = data_json['created_at']  # The timestamp of when the Tweet was created
            geo_enabled = data_json['user']['geo_enabled'] # if geo is enabled
            coordinates = data_json['coordinates'] # the coordinates of the tweet
            is_quote = data_json['is_quote_status'] # if it is quote
            
            
            # Convert the timestamp string given by Twitter to a date object. This is more easily manipulated in MongoDB.
            created = datetime.datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')
            
            # Load all of the extracted Tweet data into the variable "tweet" that will be stored into the database
            tweet = {'_id':tweet_id, 'username':username, 'followers':followers, 'tweets': tweets_number, 'text':text, 
            'geo_enabled' :geo_enabled, 'hashtags':hashtags, 'mentions': mentions, 'created':created, 'coordinates' :coordinates, 
            'is_quote':is_quote, 'is_retweet':is_retweet}

            # print the data
            print(username + ": "+text+ "\n")

            # count duplicates
            if self.collection.find_one({'_id': tweet_id}):
                self.duplicates += 1
            else:
                # insert the data into the mongoDB into a collection called collection
                self.collection.insert(tweet)
                self.data_from_stream += 1

        except Exception as e:
           print(e)

    # save data from REST API
    def save_obj_data(self, data):
        try:    
            # Decode the JSON from Twitter
            data_json = json.loads(json.dumps(data._json))

            # tweet will include full text as dependency in calling function to obtain the data
            # if retweeted the full text does not contain full text
            if 'retweeted_status' in data_json:
                text = data_json['full_text'].split(':')[0] + ': ' + data_json['retweeted_status']['full_text']
                hashtags = data_json['retweeted_status']['entities']['hashtags']  # Any hashtags used in the Tweet
                mentions = data_json['retweeted_status']['entities']['user_mentions'] # any mention used in the tweet
                is_retweet = True # if it is retweet
            else:
                text = data_json['full_text']
                hashtags = data_json['entities']['hashtags']  # Any hashtags used in the Tweet
                mentions = data_json['entities']['user_mentions'] # any mention used in the tweet
                is_retweet = False # if it is not retweet

            # Pull important data from the tweet to store in the database.
            tweet_id = data_json['id_str']  # The Tweet ID from Twitter in string format
            username = data_json['user']['screen_name']  # The username of the Tweet author
            followers = data_json['user']['followers_count']  # The number of followers the Tweet author has
            tweets_number = data_json['user']['statuses_count'] # the number of tweets and retweets of user
            dt = data_json['created_at']  # The timestamp of when the Tweet was created
            geo_enabled = data_json['user']['geo_enabled'] # if geo is enabled
            coordinates = data_json['coordinates'] # the coordinates of the tweet
            is_quote = data_json['is_quote_status'] # if it is quote

            
            # Convert the timestamp string given by Twitter to a date object. This is more easily manipulated in MongoDB.
            created = datetime.datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')
            
            # Load all of the extracted Tweet data into the variable "tweet" that will be stored into the database
            tweet = {'_id': tweet_id, 'username':username, 'followers': followers, 'tweets': tweets_number, 'geo_enabled' :geo_enabled,
             'text': text, 'hashtags':hashtags, 'mentions':mentions,'created':created, 'coordinates' :coordinates, 'is_quote':is_quote,
             'is_retweet':is_retweet}

            # print the data
            print(username + ": "+text+ "\n")

            # count duplicates
            if self.collection.find_one({'_id': tweet_id}):
                self.duplicates += 1
            else:
                # insert the data into the mongoDB into a collection called collection
                self.collection.insert(tweet)
                self.data_from_rest += 1

        except Exception as e:
           print(e)
