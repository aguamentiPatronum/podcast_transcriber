import argparse
from tabnanny import check
import pandas as pd
import json
import os
import re
from RSS_Processor import rss_to_pandas
from Audio_Processor import assemblyAI

def build_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rss", type=str, default="https://feeds.megaphone.fm/stuffyoushouldknow", \
        help="url link to a rss feed full of podcast information")
    parser.add_argument("--processed_episodes_DF", type=str, default="sysk_with_transcripts_test.parquet", \
        help="where to save, or is already saved, dataframe with information about which episodes have already been processed for future runs")
    parser.add_argument("--current_rss_file", type=str, default="", help="if a up-to-date scrape exists on the local disk, it can save time")
    parser.add_argument("--info", default=False, action='store_true', help="whether to just provide info about how many episodes are remaining to be processed, or to actually process")
    parser.add_argument("--df_compare", default=False, action='store_true', help="use when you have old DF to compare to")
    parser.add_argument("--index_based", default=False, action='store_true', help="use when you know which index of RSS df to use as first row that will be cut off")
    parser.add_argument("--start_index", type=int, default=0)
    parser.add_argument("--end_index", type=int)
    args = parser.parse_args()

    return args

# need to update this so it is only pointing to RSS data
# as all transcripts now in own DF per episode
def checkFile(args):
    if os.path.exists(args.processed_episodes_DF):
        filesize = os.path.getsize(args.processed_episodes_DF) 
        if filesize == 0:
            return pd.DataFrame()
        else:
            df = pd.read_parquet(args.processed_episodes_DF)
            return df
    else:
        return pd.DataFrame()

if __name__ == '__main__':
    args = build_args()

    if os.path.exists(args.current_rss_file):
        print("Found existing file of rss: " + str(args.current_rss_file))
        rss_df = pd.read_parquet(args.current_rss_file)
        print(rss_df.columns.tolist())
        print(rss_df.head(10))
    else:
        scrappy = rss_to_pandas.RSS_Scraper()
        scrappy.get_data()
        rss_df = scrappy.dict_to_df(file_path = 'sysk.parquet')
        print(rss_df.head(8))
    
    # show info about RSS feed
    print("Found " + str(len(rss_df)) + " episodes")
    print(rss_df)
    
    json_struct = {}
    
    if args.df_compare:
        # TODO: Load the parquet, see how long it is, and pick up where it leaves off
        # TODO: Maybe we can do this by comparing title column to the stuff in the folder? 
        # TODO: THIS STILL DOESNT WORK RIGHT
        # use this SO to merge 2 DFS: https://stackoverflow.com/questions/72693302/merge-dataframes-and-extract-only-the-rows-of-the-dataframe-that-does-not-exist
        # but answer might be to save last episode as txt file or scrape a folder for most recent chage and then only
        # process files newer than that?
        
        old_df = checkFile(args)
        print("Length of old_df: " + str(len(old_df)))
        cols_to_replace = ['transcribed']
        old_df[cols_to_replace] = 1
        # rss_df[cols_to_replace] = 0
        print(old_df)
        if len(old_df) != 0:
            col = 'id'
            rss_df.loc[rss_df[col].isin(old_df[col]), cols_to_replace] = old_df.loc[old_df[col].isin(rss_df[col]),cols_to_replace].values
            print(rss_df.iloc[600:800][['title','pubDate','transcribed']])
            print(rss_df[cols_to_replace].nunique())

            print("After merge, non-empty transcripts are: ")
            print(len(rss_df[rss_df[cols_to_replace] == 1]))
            
    if args.index_based:
        if args.end_index:
            rss_df = rss_df.iloc[args.start_index:args.end_index]
        else:
            rss_df = rss_df.iloc[args.start_index:]
        print("After drop, non-empty transcripts are: ")
        print(len(rss_df))
        print(rss_df[['title','pubDate','transcribed']])

    if not args.info:
        transcripts_list = []
        # Add a final cycle to pick up any extra rows
        pd.set_option('display.max_columns', None)
        pd.set_option('display.expand_frame_repr', False)
        pd.set_option('max_colwidth', None)

        # Try taking each row as its own df
        for step_index in range(0, len(rss_df)):
        # for step_index in range(0,20):
            temp_df = rss_df.iloc[[step_index]].copy()
            print(temp_df.index.to_list())

            # for row_index in temp_df.index.to_list():
            tmp_url = temp_df['audio_url'].values[0]
            print(tmp_url)
            tmp_title = re.sub(r'[^\w\s]', '', temp_df['title'].values[0])
            print(temp_df.head(5))
            print(temp_df.columns)
            id = temp_df['id'].values[0]
            print("Now ID is: ", id)
            print(temp_df['id'])
            tmp_json = {
                "audio_url": str(tmp_url), 
                "iab_categories": True,
                "sentiment_analysis": True,
                "auto_chapters": True,
                "entity_detection": True
            }
            transciptor = assemblyAI.AudioProcessor(json = tmp_json)
            transciptor.make_transcripts(tmp_title, id)

            headers = ['transcript']

            # headers = ['transcript', 'categories', 'chapters', 'entities']
            # sep_df_subjects = ['sentiment', 'words']

            for item in headers:
                with open(item + '.txt', 'r') as file:
                    data = file.read()
                temp_df.at[step_index, item] = data.replace("\n", "")
                print("Added new item to DF is")
                print(temp_df.at[step_index, item])

            print("Now the df is: ")
            print(temp_df.head())

            print("I: " + str(step_index))
            # try:
            temp_df.to_parquet('transcripts/sysk_with_transcripts_' + str(tmp_title) + '.parquet')
            temp_df = pd.DataFrame()
            print(temp_df.head(5))

            # for item in sep_df_subjects:
            #     with open(str(item) + '.json', 'r') as openfile:
            #         # Reading from json file
            #         json_object = json.load(openfile)
            #     with open(str(item) + '/sysk_with_transcripts_' + str(tmp_title) + "_" + str(item) + '.json', "w") as outfile:
            #         json.dump(json_object, outfile)