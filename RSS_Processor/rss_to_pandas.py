import requests
import pandas as pd
from requests_html import HTML
from requests_html import HTMLSession
import feedparser


class RSS_Scraper:

    def __init__(self, url="https://feeds.megaphone.fm/stuffyoushouldknow"):
        self.url = url
        self.df = pd.DataFrame()
        self.entries = []
        self.rss_dict = {}

    def get_feed(self):
        try:
            Feed = feedparser.parse(self.url)
            # print("Source grabbed: " + str(Feed.title))
            self.entries = Feed.entries
        except requests.exceptions.RequestException as e:
            print(e)

    def get_data(self):
        self.get_feed()

        print("Length of feed: " + str(len(self.entries)))
        # print("About to cycle through items")

        for index, item in enumerate(self.entries):
            title = item.title
            link = item.link
            description = item.description
            summary = item.summary
            pub = item.published
            pub_parsed = item.published_parsed
            id = item.id
            # print("Num of enc: " + str(len(item.enclosures)))
            if len(item.enclosures) > 1:
                enc_length = []
                enc_type = []
                enc_link = []
                for enc in item.enclosures:
                    enc_length.append(enc['length'])
                    enc_type.append(enc['type'])
                    enc_link.append(enc['link'])
            else:
                # print(item.enclosures[0])
                enc_length = item.enclosures[0]['length']
                enc_type = item.enclosures[0]['type']
                enc_link = item.enclosures[0]['href']

            self.rss_dict[index] = {'id': id, 'title': title, 'link': link, 'desc': description,
                                    'summary': summary, 'pubDate': pub, 'pubFormatted': pub_parsed,
                                    'enc_len': enc_length, 'enc_type': enc_type,
                                    'audio_url': enc_link, 'transcript': "",
                                    'categories': "", 'chapters': "", 'transcribed': ""}

    def dict_to_df(self, file_path):
        self.df = pd.DataFrame.from_dict(self.rss_dict).T
        print(self.df.head(5))
        print("Have collected info about " + str(len(self.df)) + " episodes")

        self.df['pubFormatted'] = self.df['pubFormatted'].astype(str)
        # self.df['pubFormatted']=self.df['pubFormatted'].apply(pd.Timestamp)

        self.df.to_parquet(file_path)
        return self.df
