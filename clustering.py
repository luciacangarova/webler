from nltk.tokenize import word_tokenize
import re
import string
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import collections
import stopwords

'''
    my class which organize the cluster library to cluster all the data collected
    and to store the clusters crested to the storage
'''
class Cluster:
    # custom function to tokenize a sentence and remove stopwords
    def token_structure(self):
        # structure of emoticons
        emoticons_str = r"""
            (?:
                [:=;] # Eyes
                [oO\-]? # Nose (optional)
                [D\)\]\(\]/\\OpP] # Mouth
            )"""

        # structure of other words in the tweets
        regex_str = [
            emoticons_str,
            r'<[^>]+>', # HTML tags
            r'(?:@[\w_]+)', # @-mentions
            r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
            r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
        
            r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
            r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
            r'(?:[\w_]+)', # other words
            r'(?:\S)' # anything else
        ]

        # add the structures into one variable
        tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
        return tokens_re

    def tokenize(self, s):
        tokens_re = self.token_structure()
        return tokens_re.findall(s)
 
    # split sentence and extract tokens
    def preprocess(self, s):
        # extract all tokens build with specific words for tweets
        tokens = self.tokenize(s)

        # gather stop words
        punctuation = list(string.punctuation)
        stop = stopwords.get_stopwords() + punctuation

        # return tokens without stop words
        tokens = [token.lower() for token in tokens if token.lower() not in stop]
            
        return tokens

    '''
        Functions groups data to clusters, takes arguments:
        num_clusters - number of groups to be created
        storage - my create database
    '''
    def cluster_data(self, num_clusters, storage):
        # get the values from storage
        data_values, data_keys = [] , []
        for data in storage.collection.find():
            # stores the text of the tweets
            data_values.append(data['text'])
            # store the whole data which are then inserted to the mongoDB
            data_keys.append(data) 
        
        # create vocabularly matrix and vectorizer
        ngram_vectorizer = TfidfVectorizer(sublinear_tf=True, tokenizer=self.preprocess, ngram_range=(1,2), max_features=50000)
        ngram_document_term_matrix = ngram_vectorizer.fit_transform(data_values)

        # create cluster
        my_kmeans = KMeans(n_clusters=num_clusters, verbose=10, n_init=5, max_iter=5).fit(ngram_document_term_matrix)

        # Group the posts by their cluster labels.
        clustering = collections.defaultdict(list)
        for idx, label in enumerate(my_kmeans.labels_):
            clustering[label].append(idx)

        # get tweets for each cluster and add them to clusters
        for cluster, indices in clustering.items():
            # remove all instances in clusters
            storage.db["cluster_"+str(cluster)].delete_many({})
            for index in indices:
                # add data to clusters
                storage.db["cluster_"+ str(cluster)].insert_one(data_keys[index])