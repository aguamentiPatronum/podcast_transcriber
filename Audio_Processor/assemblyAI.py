import requests
import os
import sys
from time import sleep
from dotenv import dotenv_values
import json

# RSS: https://feeds.megaphone.fm/stuffyoushouldknow

class AudioProcessor:

    def __init__(self, json):
        # load_dotenv()  # take environment variables from .env.
        self.config = dotenv_values(".env")
        # print(self.config)
        self.API_KEY = self.config['API_KEY']
        self.TRANSCRIPT_ENDPOINT = 'https://api.assemblyai.com/v2/transcript'

        self.endpoint = "https://api.assemblyai.com/v2/transcript"
        self.json = json

        # self.json = {
        #     # "audio_url": "https://www.iheart.com/podcast/105-stuff-you-should-know-26940277/episode/how-rewilding-works-94274795"
        #     # "audio_url": "https://omny.fm/shows/stuff-you-should-know-1/how-rewilding-works"
        #     "audio_url": "https://chtbl.com/track/5899E/podtrac.com/pts/redirect.mp3/traffic.omny.fm/d/clips/e73c998e-6e60-432f-8610-ae210140c5b1/a91018a4-ea4f-4130-bf55-ae270180c327/58773afb-cd6d-4bcf-b38d-ae2800c4c888/audio.mp3?utm_source=Podcast&in_playlist=44710ecc-10bb-48d1-93c7-ae270180c33e"
        # }

    def make_transcripts(self, title, episode_id):
        """_summary_

        Args:
            title (_type_): _description_
            episode_id (_type_): _description_

        Returns:
            _type_: _description_
        """
        headers = {
            "authorization": self.API_KEY,
            "content-type": "application/json"
        }

        res_transcript = requests.post(self.endpoint, json=self.json, headers=headers)
        

        res_transcript_json = res_transcript.json()
        print("Response Received")
        print(res_transcript_json['id'])


        status = ''
        while status != 'completed':    
            res_result = requests.get(
                os.path.join(self.TRANSCRIPT_ENDPOINT, res_transcript_json['id']),
                headers=headers
            )

            try:
                status = res_result.json()['status']
                # print(f'Status: {status}')

                if status == 'error':
                    sys.exit('Audio file failed to process.')
                elif status != 'completed':
                    sleep(10)
            except:
                assert(True)
        
        print("The response has been converted to JSON")
        print(res_result.json())

        OUTPUT_TRANSCRIPT_FILE = 'transcript.txt'
        with open(OUTPUT_TRANSCRIPT_FILE, 'w') as f:
            f.write(json.dumps(res_result.json()['text']))
        
        OUTPUT_TRANSCRIPT_FILE = 'categories.txt'
        with open(OUTPUT_TRANSCRIPT_FILE, 'w') as f:
            f.write(json.dumps(res_result.json()['iab_categories_result']))
        
        OUTPUT_TRANSCRIPT_FILE = 'chapters.txt'
        with open(OUTPUT_TRANSCRIPT_FILE, 'w') as f:
            f.write(json.dumps(res_result.json()['chapters']))

        # Save as own JSON
        OUTPUT_TRANSCRIPT_FILE = 'full_json/sysk_with_transcripts_' + str(title) + '.json'
        with open(OUTPUT_TRANSCRIPT_FILE, 'w') as f:
            f.write(json.dumps(res_result.json()))

        OUTPUT_TRANSCRIPT_FILE = 'words/sysk_with_transcripts_' + str(title) + '.json'
        with open(OUTPUT_TRANSCRIPT_FILE, 'w') as f:
            f.write(json.dumps(res_result.json()['words']))
        OUTPUT_TRANSCRIPT_FILE = 'sentiment/sysk_with_transcripts_' + str(title) + '.json'
        with open(OUTPUT_TRANSCRIPT_FILE, 'w') as f:
            f.write(json.dumps(res_result.json()['sentiment_analysis_results']))
        OUTPUT_TRANSCRIPT_FILE = 'entities/sysk_with_transcripts_' + str(title) + '.json'
        with open(OUTPUT_TRANSCRIPT_FILE, 'w') as f:
            f.write(json.dumps(res_result.json()['entities']))

        return res_result.json()
