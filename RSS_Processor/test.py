from process_rss_df import DF_Analyzer
from rss_to_pandas import RSS_Scraper
import pandas as pd

if __name__ == '__main__':
    old_df = pd.read_parquet('../sysk.parquet')

    scrappy = RSS_Scraper()
    scrappy.get_data()
    new_df = scrappy.dict_to_df(file_path = 'sysk_fresh_rss.parquet')

    analyzer = DF_Analyzer(old_df = old_df, new_df = new_df)
    analyzer.check_for_done('../transcripts/')

    slimmed_df = analyzer.compare_old_to_new()