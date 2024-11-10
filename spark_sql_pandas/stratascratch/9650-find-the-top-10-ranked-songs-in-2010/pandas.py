# Import your libraries
import pandas as pd

def top_10_songs_2010(billboard_top_100_year_end):
    """ Write a query that returns the top 10 songs in 2010. 
    The query should return the year_rank, group_name, and song_name. """

    return billboard_top_100_year_end[billboard_top_100_year_end['year'] == 2010]\
    .drop_duplicates(subset='year_rank')[['year_rank','group_name','song_name']].head(10)
