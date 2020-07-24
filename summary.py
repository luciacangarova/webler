from collections import Counter
from clustering import Cluster
import stopwords
from nltk import bigrams
import operator, sys
from collections import defaultdict
import matplotlib.pyplot as plt

# count the total amount of data collected in collection
def get_total_number_in(storage_collection):
    return storage_collection.count_documents({})

# get the count of tweets taken from streaming API and from the REST API
def get_number_of_tweets_from_diff_place(storage):
    return storage.data_from_stream, storage.data_from_rest

# count geo-tagged data
def get_geo_number(storage_collection):
    number = 0
    # count the tweets with coordinates
    for data in storage_collection.find():
        if data['geo_enabled'] and data['coordinates']!=None:
            number += 1
    return number

# count redudant data
def get_redudant_number(storage):
    return storage.duplicates

# count the retweets
def get_retweets_number(storage_collection):
    number = 0
    # count the retweets
    for data in storage_collection.find():
        if data['is_retweet'] :
            number += 1
    return number

# count the quotes
def get_quotes_number(storage_collection):
    number = 0
    # count the quotes
    for data in storage_collection.find():
        if data['is_quote'] :
            number += 1
    return number

# average word count per tweet
def get_average_word_count(storage_collection):
    words = 0
    tweets = 0
    max_number = 0
    min_number = sys.maxsize
    for data in storage_collection.find():
        tweets += 1
        words += len(data['text'].split(' '))
        if max_number<len(data['text'].split(' ')):
            max_number = len(data['text'].split(' '))
        if min_number>len(data['text'].split(' ')):
            min_number = len(data['text'].split(' '))
    if tweets==0:
        return 0, 0, 0
    else:
        return words/tweets, max_number, min_number

# average count of chars per tweet
def get_average_char_count(storage_collection):
    chars = 0
    tweets = 0
    max_number = 0
    min_number = sys.maxsize
    for data in storage_collection.find():
        tweets += 1
        # include also white spaces
        chars += len(data['text'])
        if max_number<len(data['text']):
            max_number = len(data['text'])
        if min_number>len(data['text']):
            min_number = len(data['text'])
    if tweets==0:
        return 0, 0, 0
    else:
        return chars/tweets, max_number, min_number

# return importanr hashtags
def get_important_hashtags(storage_collection, number):
    hashtags = []
    # collcet the hashtags
    for data in storage_collection.find():
        hashtags.extend([term['text'] for term in data["hashtags"]])
    # count the hashtags
    hashtags = Counter(hashtags)
    # plot the data
    important_hashtags = hashtags.most_common(number)
    plot_data([term[0] for term in important_hashtags], [term[1] for term in important_hashtags], 
    str(storage_collection.name)+"_hashtags", "Important hashtags in a collection, "+str(storage_collection.name), "Text of hashtags", "Count")
    # return the most common
    return important_hashtags

# return important mentiones
def get_important_mentiones(storage_collection, number):
    mentiones = []
    # collcet the mentions
    for data in storage_collection.find():
        mentiones.extend([term['screen_name'] for term in data["mentions"]])
    # count the mentions
    mentiones = Counter(mentiones)
    # plot the data
    important_mentions = mentiones.most_common(number)
    plot_data([term[0] for term in important_mentions], [term[1] for term in important_mentions], 
    str(storage_collection.name)+"_mentions", "Important mentions in a collection, "+str(storage_collection.name), "Name of mentions", "Count")
    # return the most common
    return important_mentions

# return important usernames depending on the numbre if their tweets and retweets
def get_important_usernames(storage_collection, number):
    usernames = {}
    # collect the usernames with count of posts
    for data in storage_collection.find():
        usernames[data['username']] = data['tweets']
    # plot the data
    important_usernames = Counter(usernames).most_common(number)
    plot_data([term[0] for term in important_usernames], [term[1] for term in important_usernames], 
    str(storage_collection.name)+"_tweetcount", "Active users in a collection, "+str(storage_collection.name), "Name of users", "Number of tweets and retweets")
    # return with the highest number of tweets and retweets (active users)
    return important_usernames

# return frequent words which are not stop words
def get_important_concepts(storage_collection, number):
    entities =[]
    # collect all the words except mentions and hashtags
    cluster = Cluster()
    for data in storage_collection.find():
        # tokenize the tweet and delete mentions and hashtags
        entities.extend([term for term in cluster.preprocess(data['text']) if not term.startswith(('#', '@'))])
    # count the entities
    entities = Counter(entities)
    # plot the data
    important_entities = entities.most_common(number)
    plot_data([term[0] for term in important_entities], [term[1] for term in important_entities], 
    str(storage_collection.name)+"_entities", "Important entities in a collection, "+str(storage_collection.name), "Text of concepts", "Count")
    # return the most common
    return important_entities

# return the most frequent bigrams in the collection (2 words standing next to each other in a text)
def get_frequent_bigrams(storage_collection, number):
    bigram = []
    cluster = Cluster()
    for data in storage_collection.find():
        # tokenize the tweet and delete mentions and hashtags and make bigrams
        bigram.extend(bigrams([term for term in cluster.preprocess(data['text']) if not term.startswith(('#', '@'))]))
    # count the bigrams
    bigram = Counter(bigram)
    # plot the data
    important_bigrams =  bigram.most_common(number)
    plot_data([str(term[0]) for term in important_bigrams], [term[1] for term in important_bigrams], 
    str(storage_collection.name)+"_bigrams", "Important bigrams in a collection, "+str(storage_collection.name), "Bigrams", "Count")
    # return the most common
    return important_bigrams

# return the most frequent cooccurent terms in the collection (2 words in a text)
def get_frequent_cooccurrence(storage_collection, number):
    # initialize a dictionary
    com = defaultdict(lambda : defaultdict(int))
 
    # identify the terms only without @ and #
    for data in storage_collection.find(): 
        cluster = Cluster()
        terms_only = [term for term in cluster.preprocess(data['text']) if not term.startswith(('#', '@'))]
    
        # Build co-occurrence matrix
        for i in range(len(terms_only)-1):            
            for j in range(i+1, len(terms_only)):
                w1, w2 = sorted([terms_only[i], terms_only[j]])                
                if w1 != w2:
                    com[w1][w2] += 1

    com_max = []
    # For each term, look for the most common co-occurrent terms
    for t1 in com:
        t1_max_terms = sorted(com[t1].items(), key=operator.itemgetter(1), reverse=True)[:5]
        for t2, t2_count in t1_max_terms:
            com_max.append(((t1, t2), t2_count))
    # Get the most frequent co-occurrences
    terms_max = sorted(com_max, key=operator.itemgetter(1), reverse=True)
    # plot the data
    important_terms =  terms_max[:number]
    plot_data([str(term[0]) for term in important_terms], [term[1] for term in important_terms], 
    str(storage_collection.name)+"_terms", "Important co-occurent terms in a collection, "+str(storage_collection.name), "Co-occurent terms", "Count")
    # return the most common 
    return important_terms

# plot the data in the graph for each statistics if there are data and save the figure
def plot_data(xdata, ydata, name_of_plot, title, x_title, y_title):
    ax = plt.figure()
    if xdata and ydata:
        plt.plot(xdata, ydata)
        ax.suptitle(title)
        plt.xlabel(x_title)
        plt.ylabel(y_title)
        ax.set_size_inches((15, 11), forward=False)
        plt.savefig('./plots/'+name_of_plot, dpi=500)
        #plt.show()