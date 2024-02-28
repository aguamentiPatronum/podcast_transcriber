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
    parser.add_argument("--filePath", type=str, default="sysk_with_transcripts_test.parquet", \
        help="where to save, or is already saved, dataframe with transcripts")
    parser.add_argument("--current_rss_file", type=str, default="")
    args = parser.parse_args()

    return args

def checkFile(args):
    if os.path.exists(args.filePath):
        filesize = os.path.getsize(args.filePath) 
        if filesize == 0:
            return pd.DataFrame()
        else:
            df = pd.read_parquet(args.filePath)
            return df
    else:
        return pd.DataFrame()

# def write_json(new_data, filename='data.json'):
#     # if the file exists...
#     if os.path.exists(filename):
#         with open(filename,'w') as file:
#             # First we load existing data into a dict.
#             file_data = json.load(file)
#             # Join new_data with file_data inside emp_details
#             file_data.append(new_data)
#             # Sets file's current position at offset.
#             file.seek(0)
#             # convert back to json.
#             json.dump(file_data, file, indent = 4)
#             print("Appended")
#     # if the file does not exist
#     else:
#         with open(filename, 'w') as file:
#             print("The json file is created")
#             json.dump(new_data, file, indent = 4)

if __name__ == '__main__':
    args = build_args()

    if os.path.exists(args.current_rss_file):
        print("Found existing file of rss: " + str(args.current_rss_file))
        rss_df = pd.read_parquet(args.current_rss_file)
        print(rss_df.head(10))
    else:
        scrappy = rss_to_pandas.RSS_Scraper()
        scrappy.get_data()
        rss_df = scrappy.dict_to_df(file_path = 'sysk.parquet')
    
    json_struct = {}
    # TODO: Load the parquet, see how long it is, and pick up where it leaves off
    # old_df = checkFile(args)
    # print("Length of old_df: " + str(len(old_df)))
    # if len(old_df) != 0:
    #     col = 'id'
    #     cols_to_replace = ['transcript']
    #     rss_df.loc[rss_df[col].isin(old_df[col]), cols_to_replace] = old_df.loc[old_df[col].isin(rss_df[col]),cols_to_replace].values
    #     print(rss_df.head(5))
    #     print(len(rss_df.transcript.unique()))

    #     print("After merge, non-empty transcripts are: ")
    #     print(len(rss_df[rss_df['transcript'] != ""]))

    # urls = []
    transcripts_list = []
    # print(len(rss_df))
    # to-do: pull every 10 rows and cycle those, then pull the next 10
    # chunk_size = 4

    # for step_index in range(chunk_size,len(rss_df),chunk_size):
    #     temp_df = rss_df.iloc[step_index - chunk_size : step_index, :]
    #     print(temp_df.index.to_list())

    #     for row_index in temp_df.index.to_list():
    #         tmp_url = temp_df.iloc[row_index]['audio_url']
    #         tmp_title = temp_df.iloc[row_index]['title']

    #         tmp_json = {
    #             "audio_url": str(tmp_url), 
    #             "iab_categories": True,
    #             "sentiment_analysis": True,
    #             "auto_chapters": True,
    #             "entity_detection": True
    #         }
    #         transciptor = assemblyAI.AudioProcessor(json = tmp_json)
    #         json_result = transciptor.make_transcripts()
    #         print("JSON result in build file: ")
    #         print(json_result)

    #         headers = ['transcript', 'categories', 'chapters', 'entities']
    #         sep_df_subjects = ['sentiment', 'words']

    #         for item in headers:
    #             with open(item + '.txt', 'r') as file:
    #                 data = file.read()
    #             temp_df.at[row_index, item] = data

    #         for item in sep_df_subjects:
    #             with open(item + '.txt', 'r') as file:
    #                 data = file.read()
    #             temp_df = pd.read_json(data)
    #             print("Temp df is: ")
    #             print(temp_df.head(3))
    #             temp_df.to_parquet(str(tmp_title) + "_" + str(item) + ".parquet", \
    #                 engine="fastparquet", compression="gzip")

    #         print("Now the df is: ")
    #         print(temp_df.head())

    #     print("I: " + str(step_index))
    #     temp_df.to_parquet('sysk_with_transcripts_' + str(step_index) + '.parquet', \
    #         engine="fastparquet", compression="gzip")
    #     # print("First parquet saved")
    #     # rss_df.to_parquet(args.filePath)
    #     print(temp_df.head(5))
    # Add a final cycle to pick up any extra rows
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('max_colwidth', None)

    # Try taking each row as its own df
    for step_index in range(0,1):
        temp_df = rss_df.iloc[[step_index]].copy()
        print(temp_df.index.to_list())

        # for row_index in temp_df.index.to_list():
        tmp_url = temp_df['audio_url'].values[0]
        print(tmp_url)
        tmp_title = re.sub(r'[^\w\s]', '', temp_df['title'].values[0])
        print(tmp_title)

        # tmp_json = {
        #     "audio_url": str(tmp_url), 
        #     "iab_categories": True,
        #     "sentiment_analysis": True,
        #     "auto_chapters": True,
        #     "entity_detection": True
        # }
        # transciptor = assemblyAI.AudioProcessor(json = tmp_json)
        # json_result = transciptor.make_transcripts()
        # print("JSON result in build file: ")
        # print(json_result)

        headers = ['transcript', 'categories', 'chapters', 'entities']
        sep_df_subjects = ['sentiment', 'words']

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
        temp_df.to_parquet('transcripts/sysk_with_transcripts_' + str(tmp_title) + "_" + str(step_index) + '.parquet')
        temp_df = pd.DataFrame()
        print(temp_df.head(5))

        for item in sep_df_subjects:
            with open(str(item) + '.json', 'r') as openfile:
                # Reading from json file
                json_object = json.load(openfile)
            with open(str(item) + '/sysk_with_transcripts_' + str(tmp_title) + "_" + str(item) + '.json', "w") as outfile:
                json.dump(json_object, outfile)
        # except:
        #     temp_df.to_csv('sysk_with_transcripts_' + str(tmp_title) + "_" + str(step_index) + '.csv')
        #     temp_df = pd.DataFrame()
        #     print(temp_df.head(5))