from bs4 import BeautifulSoup
import re
import csv
import requests
import sqlite3
import json
import os

def get_data():
    "No inputs. Returns a list of tuples in the format (song, artists). Uses BeautifulSoup to read the top 100 songs hummed songs in 2020."
    #Initiate empty lists to collect songs and artist information
    songs_list = []
    artist_list = []
    #Initiate empty list to collect tuples. Format: (song, artists)
    tup_list = []
    #Gets information from billboard.com using Beautiful Soup
    base_url = "https://www.billboard.com/charts/year-end/top-hummed"
    r = requests.get(base_url)
    soup = BeautifulSoup(r.text, 'html.parser')
    songs = soup.find_all("span", class_="chart-element__information__song text--truncate color--primary")
    artists = soup.find_all("span", class_="chart-element__information__artist text--truncate color--secondary")
    #Loop through Beautiful Soup results and append to lists
    for song in songs:
        songs_list.append(song.text)
    for artist in artists:
        artist_list.append(artist.text)
    #Loop through song & artist lists to create tuples. Append tuples to tup_list
    for i in range(len(songs_list)):
        tup_list.append((songs_list[i], artist_list[i]))
    return tup_list

def createDatabase(name):
    "Input: string defining database name. Returns cursor and connection to the database."
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+name)
    cur = conn.cursor()
    return cur, conn

def set_up_Billboard(cur, conn):
    "Input: Database cursor and connection. No output. Creates Table that will hold Top 100 hummed songs"
    cur.execute("CREATE TABLE IF NOT EXISTS Billboard (song_ID INTEGER PRIMARY KEY, song TEXT, artist TEXT)")
    conn.commit()

def fill_data_in_Billboard(cur,conn):
    "Input: Database cursor and connection. No output. Fills in the Billboard table with songs, artists, and ID. ID is song's unique identification number for reference."
    #Calls get_data()
    data_list = get_data()
    #Reads Billboard Database and find the last index of data in Billboard. Prevents duplicates when code is run again.
    cur.execute('SELECT song FROM Billboard')
    songs_list = cur.fetchall()
    index = len(songs_list)
    #Adds songs in Billboard table (25 entries each time)
    for i in range(25):
        #song_ID identifies unique songs
        song_ID = index + 1
        song = data_list[index][0]
        arist = data_list[index][1]
        cur.execute("INSERT OR IGNORE INTO Billboard (song_ID, song, artist) VALUES (?, ?, ?)", (song_ID, song, arist))
        index += 1
    conn.commit()


def artisticAnalytics(cur, conn):
    "Input: Database cursor and connection. Uses get_data to analyze how many times an artist had made the list. Returns tuples of top 5 artists with count."
    cur.execute('SELECT artist FROM Billboard')
    data_list = cur.fetchall()
    #Initialize Empty Dictionary
    artist_dict = {}
    #Loop through data_list and enter data in dictionary
    for artist in data_list:
        artist_dict[artist] = artist_dict.get(artist, 0) + 1
    
    sortedList = sorted(artist_dict.items(), key= lambda x: x[1], reverse =True)
    return sortedList[:5]

def writeText(filename, cur, conn):
    "Input: filename (string), database cursor, and connection. Output: Text file. Writes to file: most popular artists among MostHummed100 and their number of appearances. "
    #Call artisticAnalytics() and get_data
    popularArtists = artisticAnalytics(cur, conn)
    #Writes to text file only if length of billboard table is 100.
    cur.execute('SELECT song FROM Billboard')
    songs = cur.fetchall()

    if len(songs) == 100:
        path = os.path.dirname(os.path.abspath(__file__))
        filepath = path+'/'+filename
        f = open(filepath, "w")
        f.write("Top 5 Artists to be Featured in Google's Top Hummed Songs\n")
        f.write("----------------------------------------------------------------------\n\n")
        for i in range(len(popularArtists)):
            f.write(("{}. {} was mentioned {} times! \n").format(i+1, popularArtists[i][0], popularArtists[i][1]))
        f.write("----------------------------------------------------------------------\n\n")
        f.close()


def main():
    "Calls functions set_up_Billboard and fill_data_in_Billboard. Closes connectionn to database in the end."
    cur, conn = createDatabase('MostHummed100.db')
    set_up_Billboard(cur, conn)
    fill_data_in_Billboard(cur, conn)
    writeText("TopArtists.txt", cur, conn)
    conn.close()

if __name__ == "__main__":
    main()