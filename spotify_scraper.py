import requests
import re
import os
import sqlite3
import spotipy
import spotipy.oauth2 as oauth2
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials
import time

auth_manger = SpotifyClientCredentials(client_id = 'bef78d5ad2f34f56bd05a3a254e1121e', client_secret = '6976b0818b654915b91f5edb549c9d21')

sp = spotipy.Spotify(auth_manager = auth_manger)



def combine(cur, conn):
    """
    Input: Database cursor and connection.
    Output: Returns a list of tuples in the format (song, artist)
    Purpose: to convert song titles and artists from our existing Billboard table into an iterable data type for API scraping.
    """
    cur.execute('SELECT Billboard.song, Billboard.artist FROM Billboard')
    tupleslst = cur.fetchall()
    conn.commit()
    return tupleslst

def strip_artists(lst):
    """
    Input: The list of (song, artist) formatted tuples returned by the combine function. 
    Output: A list of primary artists per each song (takes out artists who feature/guest rap).
    Purpose: to make scraping Spotify API easier.
    """
    final_names = []
    for x in lst:
        if 'With' in x[1]:
            name = x[1].split('With')
            final_names.append(name[0].strip())
        elif '&' in x[1]:
            name = x[1].split('&')
            final_names.append(name[0].strip())
        elif 'Featuring' in x[1]:
            name = x[1].split('Featuring')
            final_names.append(name[0].strip())
        elif 'DJ' in x[1]:
            name = x[1].split('DJ')
            final_names.append(name[1].strip())
        else:
            final_names.append(x[1])
    return final_names

def strip_titles(lst):
    """
    Input: The list of (song, artist) formatted tuples returned by the combine function. 
    Output: A list of song titles without descriptions in parentheses or any extraneous words that don't align with
    songs' official titles. 
    Purpose: to make scraping Spotify API easier.
    """
    final_titles = []
    for x in lst:
        if '(' in x[0]:
            title = x[0].split('(')
            final_titles.append(title[0].strip())
        elif "\'" in x[0]:
            title = x[0].strip("\'")
            final_titles.append(title.strip())
        elif 'Megalovania' in x[0]:
            title = x[0].split('Megalovania')
            final_titles.append(title[0].strip())
        else:
            final_titles.append(x[0])

    return final_titles

def ultimate_tuple(lst):
    """
    Input: The list of (song, artist) formatted tuples returned by the combine function. 
    Output: A list of (song, artist) formatted tuples that are properly formatted and ready to be used for API scraping.
    Purpose: to make iteration during API scraping easier.
    """
    titles = strip_titles(lst)
    artists = strip_artists(lst)
    tuples = list(zip(titles, artists))
    return tuples

def fetch_popularity(x):
    """
    Input: a song title.
    Output: the song's popularity score (a metric unique to Spotify tracks used to measure popularity, between 0 and 100)
    Purpose: to fetch a given track's popularity score.
    """
    data = sp.search(q='track:' + str(x[0]))
    popscore = data['tracks']['items'][0]['popularity']
    return popscore

def pop_lst(lst):
    """
    Input: The list of (song, artist) formatted tuples returned by the combine function. 
    Output: A list of (song by artist, popularity score) formatted tuples.
    Purpose: to organize songs and artists by popularity scores in a list.
    """
    pop_lst = []
    for x in lst:
        string = x[0] + ' (by ' + x[1] + ')'
        pop_lst.append((string,fetch_popularity(x)))
    return pop_lst

def pop_table(cur, conn, lst):
    """
    Input: Database cursor, connection, and list of tuples in the format (titles, artist). 
    Output: No output. 
    Purpose: Fills in the Spotify_Popularity_Scores table with ID, track, and popularity score. 
    ID is song's unique identification number for reference.
    """
    #Sets up Spotify_Popularity_Scores table
    cur.execute("CREATE TABLE IF NOT EXISTS Spotify_Popularity_Scores (song_ID INTEGER PRIMARY KEY, track TEXT, popularity INTEGER)")
    #Calls pop_lst()
    popularity_lst = pop_lst(lst)
    #Reads Billboard Database and find the last index of data in Spotify_Popularity_Scores. Prevents duplicates when code is run again.
    cur.execute('SELECT track FROM Spotify_Popularity_Scores')
    track_list = cur.fetchall()
    index = len(track_list)
    #Adds songs in Billboard table (25 entries each time)
    for i in range(25):
        #song_ID identifies unique songs
        song_ID = index + 1
        track = popularity_lst[index][0]
        popularity = popularity_lst[index][1]
        cur.execute("INSERT INTO Spotify_Popularity_Scores (song_ID, track, popularity) VALUES (?, ?, ?)", (song_ID, track, popularity))
        index += 1
    conn.commit()

def averagePopularity(cur, conn):
    """
    Input: Database cursor and connection.
    Output: Returns average popularity score.
    Purpose: to gauge how popular the 100 most hummed songs are (according to Spotify), on average.
    """
    cur.execute('SELECT popularity FROM Spotify_Popularity_Scores')
    data_list = cur.fetchall()
    #Initiate sum
    sum = 0
    #Loop through popularity list and sum popularity scores
    for score in data_list:
        sum += score[0]
    #Caluclate Average
    num_tracks = len(data_list)
    avg = float(sum / num_tracks)
    return avg
    
def writeText(filename, lst, cur, conn):
    """
    Input: filename (string), list of tuples in the format (titles, artists), . 
    Output: Text file containing average Spotify popularity score of all 100 most hummed songs.
    Purpose: Writes to file: Average popularity score of Most Hummed Songs on Spotify. 
    """
    #Call artisticAnalytics() and get_data
    avgPopularity = averagePopularity(cur, conn)
    #Writes to text file only if length of Spotify_Popularity_Scores table is 100.  
    cur.execute('SELECT track FROM Spotify_Popularity_Scores')
    tracks = cur.fetchall()

    if len(tracks) == 100:
        path = os.path.dirname(os.path.abspath(__file__))
        filepath = path+'/'+filename
        f = open(filepath, "w")
        f.write("Average Popularity Score of Top Hummed Songs on Spotify \n")
        f.write("----------------------------------------------------------------------\n\n")
        f.write("Average Popularity Score was {}!\n".format(avgPopularity))
        f.write("----------------------------------------------------------------------\n\n")
        f.close



def main():
    """
    Calls functions combine(), ultimate_tuple(), pop_table(), and writeText(). Closes connectionn to database in the end.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/MostHummed100.db')
    cur = conn.cursor()
    tuples_lst = combine(cur, conn)
    refined_tuple_lst = ultimate_tuple(tuples_lst)
    pop_table(cur, conn, refined_tuple_lst)
    writeText("AvgSpotifyPopularity.txt", refined_tuple_lst, cur, conn)
    conn.close()


if __name__ == "__main__":
    main()