# Twitter web crawler
This project includes a code to develop a Twitter crawler for data collection (Tweets in English) and to conduct analytics. It includes:

- a crawler to access as much Twitter data as possible via streaming API and REST API
- grouping of tweets
- capturing & organising user and hashtag information through a user interaction graph
- analysing the data to generate network-based measures like ties, triads

The project aims to bring as much tweets as possible in the area of climate change and environment. It filters the tweets depending on the top 20 words in this area and it samples tweets from the top 10 most important twitter accounts, which post about these topics.


### Requirements:
In order to run the project, you have to:
1. git clone https://github.com/luciacangarova/webler.git
2. mkvirtualenv webler
3. pip install -r requirements.txt
4. download MongoDB locally on your machine
5. modify main.py with appropriate credentials for tweepy API
6. python main.py

### Output:
The main.py file will output all the information about the whole database, as well as each cluster. These information are also saved in the MongoDB in the log collection. For each appropriate function, a graph will be presented, which will be saved in a ./plots file.

### Note:
Each function is independent of the functions, therefore it is possible to get only particular interactions or statistics on any collection in any order.

