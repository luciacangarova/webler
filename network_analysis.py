from collections import Counter
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
import pandas
import igraph as ig

'''
    function which does network analysis on collection provided,
    it returns user mentions interactions and hashtags interactions,
    graph to provide how often there is a user mention interaction,
    and the functions also analyse dyads and triads
'''
def network_analysis(storage_collection, plot_data=True):
    # collect all mentions with relation to users
    user_mentions_tweets = {}
    user_mentions_retweets = {}
    user_mentions_quotes = {}

    # collect all hashtags with relation to another hashtags
    user_hashtags_tweets = []
    user_hashtags_retweets = []
    user_hashtags_quotes = []

    # collect all dates when mention was created
    dates_mentions_tweets = []
    dates_mentions_retweets = []
    dates_mentions_quotes = []
    
    # collect apropriate data from each tweet/retweet/quote
    for data in storage_collection.find():
        
        if data['is_quote']:
            # track when there is a mention in a quote
            if data['mentions']:
                dates_mentions_quotes.extend([data['created'] for term in data["mentions"]])
            
            # append pair of hashtags in a quote
            user_hashtags_quotes.extend([(x['text'],y['text']) for x in data['hashtags'] for y in data['hashtags'] if x['text']!=y['text']])
            
            # append data about the user mentions for the user who created the quote
            if data['username'] in user_mentions_quotes:
                user_mentions_quotes[data['username']] += [term['screen_name'] for term in data["mentions"]]
            else:
                user_mentions_quotes[data['username']] = [term['screen_name'] for term in data["mentions"]]
        
        elif data['is_retweet']:
            # track when there is a mention in a retweet
            if data['mentions']:
                dates_mentions_retweets.extend([data['created'] for term in data["mentions"]])
            
            # append pair of hashtags in a retweet
            user_hashtags_retweets.extend([(x['text'],y['text']) for x in data['hashtags'] for y in data['hashtags'] if x['text']!=y['text']])
            
            # append data about the user mentions for the user who created the retweet
            if data['username'] in user_mentions_retweets:
                user_mentions_retweets[data['username']] += [term['screen_name'] for term in data["mentions"]]
            else:
                user_mentions_retweets[data['username']] = [term['screen_name'] for term in data["mentions"]]
        
        else:
            # track when there is a mention in a tweet
            if data['mentions']:
                dates_mentions_tweets.extend([data['created'] for term in data["mentions"]])

            # append pair of hashtags in a tweet
            user_hashtags_tweets.extend([(x['text'],y['text']) for x in data['hashtags'] for y in data['hashtags'] if x['text']!=y['text']])
            
            # append data about the user mentions for the user who created the tweet
            if data['username'] in user_mentions_tweets:
                user_mentions_tweets[data['username']] += [term['screen_name'] for term in data["mentions"]]
            else:
                user_mentions_tweets[data['username']] = [term['screen_name'] for term in data["mentions"]]

    # count the frequencies of mentions with relation to users for each group ( tweet, reteet, quote)
    user_interactions_tweets = {}
    user_interactions_retweets = {}
    user_interactions_quotes = {}
    for keys, values in user_mentions_tweets.items():
        user_interactions_tweets[keys] = Counter(values)
    for keys, values in user_mentions_retweets.items():
        user_interactions_retweets[keys] = Counter(values)
    for keys, values in user_mentions_quotes.items():
        user_interactions_quotes[keys] = Counter(values)

    # remove duplicates from the hashtags interaction list
    user_hashtags_retweets = list(set(user_hashtags_retweets))
    user_hashtags_tweets = list(set(user_hashtags_tweets))
    user_hashtags_quotes = list(set(user_hashtags_quotes))
    
    # function option from argument to plot data (when false the program is then faster)
    if plot_data:
        # plot the data for user interaction
        plot_directed_graph(user_interactions_tweets, "Interaction between users mentions in tweets", 
        str(storage_collection.name)+"_user_interactions_tweets")
        plot_directed_graph(user_interactions_retweets, "Interaction between users mentions in retweets",
        str(storage_collection.name)+"_user_interactions_retweets")
        plot_directed_graph(user_interactions_quotes, "Interaction between users mentions in quotes",
        str(storage_collection.name)+"_user_interactions_quotes")

        # plot the data for hashtag interactions
        plot_undirected_graph(user_hashtags_tweets, "Interaction between hashtags in tweets", 
        str(storage_collection.name)+"_hashtags_interactions_tweets")
        plot_undirected_graph(user_hashtags_retweets, "Interaction between hashtags in retweets",
        str(storage_collection.name)+"_hashtags_interactions_retweets")
        plot_undirected_graph(user_hashtags_quotes, "Interaction between hashtags in quotes",
        str(storage_collection.name)+"_hashtags_interactions_quotes")

    # plot data when the mentions were created
    for item, for_what in [(dates_mentions_tweets, "tweets"), (dates_mentions_retweets, "retweets"), (dates_mentions_quotes, "quotes")]:
        # a list of "1" to count the mentions
        ones = [1]*len(item)
        # the index of the series
        idx = pandas.DatetimeIndex(item)
        # the actual series (at series of 1s for the moment)
        mentions_data = pandas.Series(ones, index=idx)
        # Resampling / bucketing
        per_day = mentions_data.resample('D').sum().fillna(0)
        print(per_day)
        # plot data
        plot_time_results(per_day,"Number of ties in a day, for " + for_what, "Day", "Count", 
        str(storage_collection.name)+"_ties_"+for_what)

    # collect all triads and dyads for tweets, retweets and quotes
    dyads = {}
    triads = {}
    for data, for_what in [(user_interactions_tweets, "tweets"), (user_interactions_retweets, "retweets"), (user_interactions_quotes, "quotes")]:
        # create graph to analyze triads
        G = nx.DiGraph()
        for key, value in data.items():
            for item in value.items():
                G.add_edges_from([(key, item[0])], weight=item[1])
        triad_dict = nx.triadic_census(G)
        
        # create list of edges between users
        tuples =[]
        for key, value in data.items():
            for item in value.items():
                tuples.append((key, item[0]))
        # create graph to analyze ties
        G = ig.Graph.TupleList(tuples, directed = True)
        # get all dyads
        dc = G.dyad_census()
        dyad_dict = dc.as_dict()

        # append results
        dyads[for_what] = dyad_dict
        triads[for_what] = triad_dict

    print("Tweets mentions: ", user_interactions_tweets)
    print("Retweets mentions: ", user_interactions_retweets)
    print("Quotes mentions: ", user_interactions_quotes)
    print()
    print("Tweets hashtags: ", user_hashtags_tweets)
    print("Retweets hashtags: ", user_hashtags_retweets)
    print("Quotes hashtags: ", user_hashtags_quotes)
    print()
    print("Tweets ties count: ", len(dates_mentions_tweets))
    print("Retweets ties count: ", len(dates_mentions_retweets))
    print("Quotes ties count: ", len(dates_mentions_quotes))
    print()
    print("Number of dyads: ", dyads)
    print("Number of triads: ", triads)

    # return data to be saved in log of mongoDB
    results = [
        user_interactions_tweets, user_interactions_retweets, user_interactions_quotes, 
        user_hashtags_tweets, user_hashtags_retweets, user_hashtags_quotes,
        dates_mentions_tweets, dates_mentions_retweets, dates_mentions_quotes,
        dyads, triads
        ]

    return results

# function to plot data and create directed mention interaction graph between users
def plot_directed_graph(data, title, name_of_plot):
    # if there are some data, plot data in directed graph and save the figure
    if data:
        G = nx.DiGraph()
        for key, value in data.items():
            for item in value.items():
                G.add_edges_from([(key, item[0])], weight=item[1])
        edge_labels=dict([((u,v,),d['weight']) for u,v,d in G.edges(data=True)])
        pos=nx.spring_layout(G)
        nx.draw_networkx_edge_labels(G,pos,edge_labels=edge_labels)
        nx.draw(G, pos, node_size=150,font_size=10, with_labels = True, arrowsize=15, width=2, edge_color="green", node_color="lightgreen", font_weight="bold")
        plt.title(title)
        fig = plt.gcf()
        fig.set_size_inches((15, 11), forward=False)
        plt.savefig('./plots/'+name_of_plot, dpi=500)
        #plt.show()
        plt.clf()

# function to plot undirected hashtag interaction graph
def plot_undirected_graph(data, title, name_of_plot):
    # if there are some data, plot data in undirected graph and save the figure
    if data:
        G = nx.Graph()
        for item in data:
            G.add_edges_from([(item[0], item[1])])
        edge_labels=dict([((u,v,),"") for u,v in G.edges()])
        pos=nx.shell_layout(G)
        nx.draw_networkx_edge_labels(G,pos, edge_labels=edge_labels)
        nx.draw(G, pos, node_size=150,font_size=10, with_labels = True, arrowsize=15, width=2, edge_color="green", node_color="lightgreen", font_weight="bold")
        plt.title(title)
        fig = plt.gcf()
        fig.set_size_inches((15, 11), forward=False)
        plt.savefig('./plots/'+name_of_plot, dpi=500)
        #plt.show()
        plt.clf()

# function to plot time data when mentions were created between users and how often
def plot_time_results(data, title, x_title, y_title, name_of_plot):
    # if there are some data plot a line graph and save the figure
    if data.any():
        fig, ax = plt.subplots()
        ax.set(xlabel=x_title, ylabel=y_title, title=title)
        data.plot.line(style='o-')
        fig.set_size_inches((15, 11), forward=False)
        plt.savefig('./plots/'+name_of_plot, dpi=500)
        #plt.show()