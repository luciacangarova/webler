from crawler import Crawler
from storage import Storage
from clustering import Cluster
import summary, network_analysis
import time, math
import threading

''' PART 1 - collect data'''

# tweepy credentials
_access_token = ""
_access_secret =""
_consumer_key = ""
_consumer_secret = ""

# create new storage instance
my_storage = Storage('mongodb://localhost:27017/')

# create crawler, specified with words you want to search and users you want to track and time limit in seconds
words = ['eco', 'climate', 'global warming', 'climate change', 'earth', 'global', 'planet', 'deforestation', 'change', 'sustainability',
'depolarization', 'mutate', 'plastics', 'eco-friendly', 'vegan', 'vegetarian', 'zero-waste', 'recycle', 'enviromentalists', 'reduce']
people = ['Greenpeace', 'WWF', 'foe_us', 'TheGreenParty', 'peopleandplanet', 'ClimateCentral', 'CIimateFacts', 'GretaThunberg']
# time limit in seconds
time_limit = 3600
crawler = Crawler(_access_token, _access_secret, _consumer_key, _consumer_secret, my_storage, words, people, time_limit)

# create API defined on the crawler with proper authentication
crawler.create_API()
# create streamer defined on the crawler with proper authentication
crawler.create_streamer() 


def run_REST():
    # run REST API streaming for 1 hour
    start_time = time.time()
    while (time.time() - start_time) < time_limit:
        crawler.run_rest_search()
        crawler.run_rest_user_timeline()

def run_STREAM():
    # run streaming API for 1 hour (can be specified in simple_listener.py)
    crawler.run_streamer()

# class for threads to run streaming and REST API in parallel
class myThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        print ("Starting " + self.name)
        if self.threadID==0:
            run_STREAM()
        elif self.threadID==1:
            run_REST()
        print ("Exiting " + self.name)

# run streming API alongside with REST API
# Create new threads
thread1 = myThread(0, "Streaming API")
thread2 = myThread(1, "REST API")

# Start new Threads
thread1.start()
thread2.start()
# wait on both threads to finish
thread1.join()
thread2.join()


''' PART 2 - group data'''

# cluster data into groups
clustering = Cluster()
# get the one percent of all tweets
one_percent = int(summary.get_total_number_in(my_storage.collection) * 0.01)
my_storage.log_collection.insert_one({"_id":"Whole data", "Number of clusters:": one_percent})
# cluster the data and measure time how long it took
start = time.time()
clustering.cluster_data(one_percent, my_storage)
end = time.time()
print("Time taken to cluster data: ", end - start)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": {'Cluster Time': (end - start)}})


''' PART 3 and 4 - summarize the data and do interaction graph and network analysis'''

# summary the whole data with top 10 data
print()
print("---------------------------------------------------------------------------------------------------")
print("Data about the whole database: ")
print("---------------------------------------------------------------------------------------------------")
# total tweets which were collected
results = summary.get_total_number_in(my_storage.collection)
print("Total count: ", results)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": {'Count': results}})

# get the number of tweets collected from streaming API and from REST API
results = summary.get_number_of_tweets_from_diff_place(my_storage)
print("Number of tweets from streaming API: ", results[0])
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": {'Number of tweets from streaming API': results[0]}})
print("Number of tweets from REST API: ", results[1])
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": {'Number of tweets from REST API': results[1]}})

# get the most top 10 frequent hashtags in the whole data
results = summary.get_important_hashtags(my_storage.collection, 10)
print("Important hashtags: ", results)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": {'Important hashtags': results}})

# get the most top 10 mentiones in the whole data
results = summary.get_important_mentiones(my_storage.collection, 10)
print("Important mentions: ", results)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": { 'Important mentions': results}})

# get the most top 10 users in the whole data depending on the number of tweets and retweets
results = summary.get_important_usernames(my_storage.collection, 10)
print("Important usernames: ", results)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": { 'Important usernames': results}})

# get the most top 10 frequent words in the whole data
results = summary.get_important_concepts(my_storage.collection, 10)
print("Important concept/entities: ", results)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": {'Important concepts/entities': results}})

# returns how many duplicated tweets were returned
results = summary.get_redudant_number(my_storage)
print("Redudant number of tweets: ", results)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": {'Redudant number of tweets': results}})

# get how many tweets were geo-tagged
results = summary.get_geo_number(my_storage.collection)
print("Geo-Tagged number of tweets: ", results)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": { 'Geo-Tagged number of tweets': results}})

# get how many retweets we collected
results = summary.get_retweets_number(my_storage.collection)
print("Retweeted number of tweets: ", results)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": { 'Retweeted number of tweets': results}})

# get how many quotes we collected
results = summary.get_quotes_number(my_storage.collection)
print("Quoted number of tweets: ", results)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": { 'Quoted number of tweets': results}})

# get the average count of words per tweet
results, maximum, minimum = summary.get_average_word_count(my_storage.collection)
print("Average word count per tweet: ", results)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": { 'Average word count per tweet': results,
"Maximum word count per tweet": maximum, "Minimum word count per tweet":minimum}})

# get the average count of characters per tweet
results, maximum, minimum = summary.get_average_char_count(my_storage.collection)
print("Average char count per tweet: ", results)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": { 'Average char count per tweet': results,
"Maximum char count per tweet": maximum, "Minimum char count per tweet":minimum}})

# get the most frequent bigrams in the whole data (2 words which stands next to each other in the text)
results = summary.get_frequent_bigrams(my_storage.collection, 10)
print("Frequent bigrams: ", results)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": { 'Frequent bigrams': results}})

# get the most frequent co-occurent terms in the whole data
results = summary.get_frequent_cooccurrence(my_storage.collection, 10)
print("Frequent co-occurent terms: ", results)
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": {'Frequent co-occurent terms': results}})


# user interaction networks - mentions and hashtags for each group - tweets (which are not retweets or quotes), retweets and quotes
# get the ties/links if user1 mentiones user2 in tweets, retweets and quotes
# triads if user1, user2 and user3 have mentiones between each other in tweets, retweets and quotes
results = [
        "user_interactions_tweets", "user_interactions_retweets", "user_interactions_quotes", 
        "user_hashtags_tweets", "user_hashtags_retweets", "user_hashtags_quotes",
        "dates_mentions_tweets", "dates_mentions_retweets", "dates_mentions_quotes",
        "dyads", "triads"
        ]

data = []
data.extend(network_analysis.network_analysis(my_storage.collection))
for i in range(len(results)):
    my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": {results[i]: data[i]}})

# statistics about the individual clusters
print("---------------------------------------------------------------------------------------------------")
print("Data about the individual clusters: ")
print("---------------------------------------------------------------------------------------------------")
print("---------------------------------------------------------------------------------------------------")
average_number = 0
max_number = 0
min_number = summary.get_total_number_in(my_storage.collection)
max_cluster = ""
min_cluster = ""
for i in range(one_percent):
    # get the name of the cluster in mongoDB
    instance = my_storage.db["cluster_"+ str(i)]
    name = str(instance.name)
    number = summary.get_total_number_in(instance)
    # extract the biggest and smallest cluster
    if number>max_number:
        max_number = number
        max_cluster = name
    if number<min_number:
        min_number = number
        min_cluster = name
    # count the average number of tweets
    average_number += number
    # if we do not have garbage cluster do the interaction and network analysis
    if(summary.get_total_number_in(instance)>20):
        # returns the number of data in the cluster
        results = summary.get_total_number_in(instance)
        print("Total count: ", results)
        my_storage.log_collection.insert_one({"_id":name, 'Count': results})

        # returns the most top 10 frequent hashtags in the cluster
        results = summary.get_important_hashtags(instance, 10)
        print("Important hashtags: ", results)
        my_storage.log_collection.update_one({"_id":name}, { "$set": {'Important hashtags': results}})

        # returns the most top 10 frequent mentions in the cluster
        results = summary.get_important_mentiones(instance, 10)
        print("Important mentions: ", results)
        my_storage.log_collection.update_one({"_id":name}, { "$set": { 'Important mentions': results}})

        # returns the most top 10 important users in the cluster depending on the number of their tweets and retweets
        results = summary.get_important_usernames(instance, 10)
        print("Important usernames: ", results)
        my_storage.log_collection.update_one({"_id":name}, { "$set": { 'Important usernames': results}})

        # returns the most top 10 frequent concepts/entities in the cluster
        results = summary.get_important_concepts(instance, 10)
        print("Important concept/entities: ", results)
        my_storage.log_collection.update_one({"_id":name}, { "$set": { 'Important concepts/entities': results}})

        # returns the count of geotagged data
        results = summary.get_geo_number(instance)
        print("Geo-Tagged number of tweets: ", results)
        my_storage.log_collection.update_one({"_id":name}, { "$set": {'Geo-Tagged number of tweets': results}})

        # returns the number of retweets in the cluster
        results = summary.get_retweets_number(instance)
        print("Retweeted number of tweets: ", results)
        my_storage.log_collection.update_one({"_id":name}, { "$set": {'Retweeted number of tweets': results}})

        # returns the number of quotes in the cluster
        results = summary.get_quotes_number(instance)
        print("Quoted number of tweets: ", results)
        my_storage.log_collection.update_one({"_id":name}, { "$set": { 'Quoted number of tweets': results}})

        # returns the average word count per tweet in the cluster
        results, maximum, minimum = summary.get_average_word_count(instance)
        print("Average word count per tweet: ", results)
        my_storage.log_collection.update_one({"_id":name}, { "$set": { 'Average word count per tweet': results, 
        "Maximum word count per tweet": maximum, "Minimum word count per tweet":minimum}})

        # returns the average character count per tweet in the cluster
        results, maximum, minimum = summary.get_average_char_count(instance)
        print("Average char count per tweet: ", results)
        my_storage.log_collection.update_one({"_id":name}, { "$set": { 'Average char count per tweet': results,
        "Maximum char count per tweet": maximum, "Minimum char count per tweet":minimum}})

        # returns the most top 10 frequent bigrams in the cluster
        results = summary.get_frequent_bigrams(instance, 10)
        print("Frequent bigrams: ", results)
        my_storage.log_collection.update_one({"_id":name}, { "$set": { 'Frequent bigrams': results}})

        # returns the most top 10 frequent co-occurent terms in the cluster
        results = summary.get_frequent_cooccurrence(instance, 10)
        print("Frequent co-occurent terms: ", results)
        my_storage.log_collection.update_one({"_id":name}, { "$set": {'Frequent co-occurent terms': results}})


        # user interaction networks - mentions and hashtags for each group - tweets (which are not retweets or quotes), retweets and quotes
        # get the ties/links if user1 mentiones user2 in tweets, retweets and quotes
        # triads if user1, user2 and user3 have mentiones between each other in tweets, retweets and quotes
        results = [
                "user_interactions_tweets", "user_interactions_retweets", "user_interactions_quotes", 
                "user_hashtags_tweets", "user_hashtags_retweets", "user_hashtags_quotes",
                "dates_mentions_tweets", "dates_mentions_retweets", "dates_mentions_quotes",
                "dyads", "triads"
                ]

        data = []
        data.extend(network_analysis.network_analysis(instance, False))
        for i in range(len(results)):
            my_storage.log_collection.update_one({"_id":name}, { "$set": { results[i]: data[i]}})
        print("---------------------------------------------------------------------------------------------------")

print("---------------------------------------------------------------------------------------------------")
# get the average count of tweets/retweets/quotes per cluster
mean = average_number/one_percent 
print("Average number of tweets in a cluster: ", mean)
print("The biggest cluster: ", max_cluster, "with", max_number, "tweets")
print("The smallest cluster: ", min_cluster, "with", min_number, "tweet(s)")
# get the standard deviation
variance = 0
for i in range(one_percent):
    instance = my_storage.db["cluster_"+ str(i)]
    number = summary.get_total_number_in(instance)
    name = str(instance.name)
    variance += (mean-number)**2
print("The standard deviation of the number of tweets per cluster: ", math.sqrt(variance/one_percent))
my_storage.log_collection.update_one({"_id":"Whole data"}, { "$set": {"Average number of tweets in a cluster": average_number/one_percent,
"The biggest cluster" : (max_cluster, max_number), "The smallest cluster" : (min_cluster, min_number),
"The standard deviation of the number of tweets per cluster": math.sqrt(variance/one_percent)}})
print("---------------------------------------------------------------------------------------------------")
