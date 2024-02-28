import pandas as pd
import os
import re


class DF_Analyzer:

    def __init__(self, new_df, old_df):
        self.new_df = new_df
        self.old_df = old_df
        print(old_df.columns)
        if ('transcribed' not in self.old_df.columns):
            print("Old df missing transcribed column")
            self.old_df['transcribed'] = ""

    # Need a way to verify which episodes have already been
    # transcribed prior to comparing to newest RRS feed data
    def check_for_done(self, file_dir):
        """_summary_

        Args:
            file_dir (_type_): _description_
        """
        # for each fow of the dataframe with RSS info
        for index in range(0, len(self.old_df)):
            # If that row isn't already marked as done
            if (self.old_df.iloc[index, ]['transcribed'] != 1):
                # grab that episode's title
                episode_title = self.old_df.iloc[index, ]['title'].lower()
                # episode_title = episode_title.replace("-","").replace(":","").replace("?","").replace(",","").replace("!","").replace("'","").replace("...","")
                episode_title = re.sub(r'[^\w\s]', '', episode_title)
                print("Now episode title is saved as: " + str(episode_title))

                # cycle through the given directory
                for root, subdirs, files in os.walk(file_dir):
                    # check each file in the directory
                    for filename in files:
                        # if the episode's title is in a filename
                        if episode_title in filename.lower():
                            # mark that this episode has been transcribed
                            print("Found file for episode: " +
                                  str(episode_title))
                            self.old_df.at[index, 'transcribed'] = 1
                            # stop checking for this episode
                            # break
        print("Now we have looked for transcribed items: ")
        print(self.old_df.head(60))

    # Look at new RSS data and compare to older RSS data
    # Figure out which episodes are newly released
    # And any that are old but not yet captured
    # Remove all other data from the New Dataframe
    def compare_old_to_new(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        # Merge old and new DFs
        combo_df = self.new_df.append(self.old_df).drop_duplicates(
            subset='title', keep="last")

        # drop transcribed column from new df
        combo_df = combo_df[combo_df['transcribed'] != 1]
        print("Have combined the RSS DFs: ")
        print(combo_df[['title', 'transcribed']].head(25))

        # save this combined DF to the file for future reference
        combo_df.to_parquet('../sysk_remaining.parquet')

        print("Now working from df: ")
        print(combo_df.head(35))
        print("Of length: " + str(len(combo_df)))
        return combo_df

        # col = 'id'
        # cols_to_replace = ['transcribed']
        # rss_df.loc[rss_df[col].isin(old_df[col]), cols_to_replace] = old_df.loc[old_df[col].isin(rss_df[col]),cols_to_replace].values
        # print(rss_df.head(5))
        # print(len(rss_df.transcript.unique()))

        # print("After merge, non-empty transcripts are: ")
        # print(len(rss_df[rss_df['transcribed'] != 1]))
