#importing the necessary libraries
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo
from googleapiclient.discovery import build
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
uri = "mongodb+srv://chithu2404:raji@cluster0.nsdnzaj.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client.youtube_data
coll=db.youtube

# BUILDING CONNECTION WITH YOUTUBE API
api_service_name = 'youtube'
api_version = 'v3'
api_key = 'AIzaSyCRReAKEOLdDQCN7RfuMXmWurLKzEBLdgk'
youtube = build(api_service_name,api_version,developerKey =api_key)
channel_id=['UCWvrhwQpW4MNCWKzTz_Qh3g',
            'UCXdZNHV74WIzwR07T3fy8qg',
            'UCzT9Hm342UtKel4XG5qh4Qw', 
            'UCz5o6ePrbmmnKa9fGZP6tHQ',
            'UCOe2svFdokRH2l0lvQURbqg',
            'UCnVpEcfut-Bu1IFmQr7vRuw',
            'UCwr-evhuzGZgDFrq_1pLt_A',
            'UC3CWkAYRbqUFLq6wQF-VyPw',
            'UCtYIA8Wxbt-tvo9Ovyow6xg',
            'UCtfzxaW5ua7X0NfYn-RSx3w']

# CONNECTING WITH MYSQL DATABASE
import mysql.connector
mydb = mysql.connector.connect(host="localhost",user="root",password="")
mycursor = mydb.cursor(buffered=True)

# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    channel_data=[]
    channel_response=youtube.channels().list(part='snippet,contentDetails,statistics',id=channel_id).execute()

    for i in range(len(channel_response['items'])):

        data=dict(channel_name=channel_response['items'][i]['snippet']['title'],
                channel_id=channel_response['items'][i]['id'],
                subscribers=channel_response['items'][i]['statistics']['subscriberCount'],
                channel_views= channel_response['items'][i]['statistics']['viewCount'],
                channel_description=channel_response['items'][i]['snippet']['description'],
                playlist_id=channel_response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                videos_count=channel_response['items'][i]['statistics']['videoCount'],
                Joined_on=channel_response['items'][i]['snippet']['publishedAt']
                )
        channel_data.append(data)
    return channel_data

    
# FUNCTION TO GET PLAYLIST IDs          
def get_playlist_details(channel_id):
    playlist_data=[]
    playlist_response=youtube.channels().list(id=channel_id,part='snippet,contentDetails').execute()
    next_page=None

    while True:
        playlist_response=youtube.playlists().list(part='snippet,contentDetails',channelId=channel_id,maxResults=50,pageToken=next_page).execute()

        for playlist in playlist_response['items']:
            data=dict(playlist_id=playlist['id'],
                    channel_id=playlist['snippet']['channelId'],
                    playlist_name=playlist['snippet']['title'],
                    playlist_description=playlist['snippet'].get('description'),
                    publishedAt=playlist['snippet']['publishedAt']
                    )
            playlist_data.append(data)
        next_page_token=playlist_response.get('nextPageToken')

        if next_page_token is None:
            break
    return playlist_data


# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids=[]
    video_response=youtube.channels().list(id=channel_id,part='contentDetails').execute()
    playlist_id=video_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None

    while True:
        video_response=youtube.playlistItems().list(playlistId=playlist_id,part='snippet',maxResults=50,pageToken=next_page_token).execute()

        for i in range(len(video_response['items'])):
            video_ids.append(video_response['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=video_response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# FUNCTION CONVERT TO DURATION TIME
def extract_time_components(duration_str):
    duration_str=duration_str[2:]
    hours,minutes,seconds=0,0,0
    parts=duration_str.split('H')
    if len(parts)>1:
        hours=int(parts[0])
        duration_str=parts[1]

    parts=duration_str.split('M')
    if len(parts)>1:
        minutes=int(parts[0])
        duration_str=parts[1]
        
    if 'S' in duration_str:
        seconds=int(duration_str.split('S')[0])
            
    hours_str=str(hours).zfill(2)
    minutes_str=str(minutes).zfill(2)
    seconds_str=str(seconds).zfill(2)

    return ":".join([hours_str,minutes_str,seconds_str])

# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids):
    video_stats=[]

    for i in range(0,len(v_ids),50):
        response=youtube.videos().list(part='snippet,contentDetails,statistics',id=','.join(v_ids[i:i+50])).execute()

        for video in response['items']:
            video_details=dict(channel_id=video['snippet']['channelId'],
                               channel_name=video['snippet']['channelTitle'],
                                video_id=video['id'],
                                video_title=video['snippet']['title'],
                                video_description=video['snippet']['description'],
                                duration=extract_time_components(video['contentDetails']['duration']),
                                view_count=video['statistics']['viewCount'],
                                likes_count=video['statistics'].get('likeCount',0),
                                dislikes_count=video['statistics'].get('dislikeCount',0),
                                definition=video['contentDetails']['definition'],
                                favorite_count=video['statistics']['favoriteCount'],
                                comment_count=video['statistics'].get('commentCount',0),
                                publishedat=video['snippet'].get('publishedAt')
                                )
            video_stats.append(video_details)
    return video_stats


# FUNCTION TO GET COMMENT DETAILS
def get_comment_details(video_ids):
    comment_data=[]
    for i in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=i)

            response = request.execute()
            for cmt in response['items']:
                data=dict(channel_id=cmt['snippet']['topLevelComment']['snippet']['channelId'],
                            comment_id=cmt['id'],
                            video_id=cmt['snippet']['videoId'],
                            comment_Author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_Text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_likes=cmt['snippet']['topLevelComment']['snippet']['likeCount']
                            )
                comment_data.append(data)
        except:
            pass

    return comment_data


# FUNCTION TO GET FULL CHANNEL DETAILS
def channel(channel_id):
    c=get_channel_details(channel_id)
    p=get_playlist_details(channel_id)
    v=get_channel_videos(channel_id)
    vd=get_video_details(v)
    cm=get_comment_details(v)
    data={'channel_details':c,
            'playlist_details':p,
            'video_details':vd,
            'comment_details':cm}
    return data

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_name():   
        ch_name = []
        for i in db.youtube.find():
            ch_name.append(i["channel_details"][0]['channel_name'])
        return ch_name

# Create a new database and use
mycursor.execute("CREATE DATABASE IF NOT EXISTS ytube")
mycursor.execute("USE ytube")

# Create a new table
mycursor.execute(''' CREATE TABLE IF NOT EXISTS channeldetails (channel_name VARCHAR(255),
                                                    channel_id VARCHAR(255),
                                                    subscribers INT,
                                                    channel_views INT,
                                                    channel_description TEXT,
                                                    playlist_id VARCHAR(255),
                                                    videos_count INT,
                                                    Joined_on DATETIME)''')

mycursor.execute(''' CREATE TABLE IF NOT EXISTS playlistdetails (playlist_id VARCHAR(255),
                                                    channel_id VARCHAR(255),
                                                    playlist_name VARCHAR(255),
                                                    playlist_description TEXT,
                                                    publishedAt DATETIME)''')

mycursor.execute(''' CREATE TABLE IF NOT EXISTS videodetails (channel_id VARCHAR(255),                                                    
                                                    channel_name VARCHAR(255),
                                                    video_id VARCHAR(255),
                                                    video_title VARCHAR(255),
                                                    video_description TEXT,
                                                    duration TIME,
                                                    view_count INT,
                                                    likes_count INT,
                                                    dislikes_count INT,
                                                    definition TEXT,
                                                    favorite_count INT,
                                                    comment_count INT,
                                                    publishedat DATETIME)''')

mycursor.execute(''' CREATE TABLE IF NOT EXISTS commentsdetails (channel_id VARCHAR(255),
                                                    comment_id VARCHAR(255),
                                                    video_id VARCHAR(255),
                                                    comment_Author VARCHAR(255),
                                                    comment_Text TEXT,
                                                    comment_likes INT)''')
#FUNCTION INSERT VALUES TO SQL
def sql_main(channel_name):
    sql_c= '''INSERT INTO channeldetails (channel_name,channel_id,subscribers,channel_views,channel_description,playlist_id,videos_count,Joined_on)VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'''
    val=tuple(m['channel_details'][0].values())
    mycursor.execute(sql_c, val)

    sql_p= '''INSERT INTO playlistdetails (playlist_id,channel_id,playlist_name,playlist_description,publishedAt)VALUES(%s,%s,%s,%s,%s)'''
    for i in m['playlist_details']:
        val=tuple(i.values())
        mycursor.execute(sql_p, val)

    sql_v= '''INSERT INTO videodetails (channel_id,channel_name,video_id,video_title,video_description,duration,view_count,likes_count,dislikes_count,definition,favorite_count,comment_count,publishedat)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    for j in m['video_details']:
        val=tuple(j.values())
        mycursor.execute(sql_v, val)

    sql_cm= '''INSERT INTO commentsdetails (channel_id,comment_id,video_id,comment_Author,comment_Text,comment_likes)VALUES(%s,%s,%s,%s,%s,%s)'''
    for k in m['comment_details']:
        val=tuple(k.values())
        mycursor.execute(sql_cm, val)
    mydb.commit()

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["DATA COLLECTION","SELECT AND STORE","DATA ANALYSIS"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="vertical")
st.title(':violet[Youtube Data Harvesting]') 

#VIEW DATA COLLECTION PAGE
if selected == "DATA COLLECTION": 

 def collection_data():
    return pd.DataFrame(
        {
            "Channel Name": ['Best Prime Stories Tamil','Arunkumar Bairavan','Aram foods','SMARTEN UP learn with dhiya', 
                             'Market Tamizha','Deep Matrix','Error Makes Clever Academy',
                             'BTree Systems','Thoufiq M','Tiny Medicine'],
            "Channel Id": ['UCWvrhwQpW4MNCWKzTz_Qh3g','UCXdZNHV74WIzwR07T3fy8qg','UCzT9Hm342UtKel4XG5qh4Qw', 'UCz5o6ePrbmmnKa9fGZP6tHQ',
                            'UCOe2svFdokRH2l0lvQURbqg','UCnVpEcfut-Bu1IFmQr7vRuw',
                            'UCwr-evhuzGZgDFrq_1pLt_A','UC3CWkAYRbqUFLq6wQF-VyPw','UCtYIA8Wxbt-tvo9Ovyow6xg',
                            'UCtfzxaW5ua7X0NfYn-RSx3w'],
        }
    )
 df = collection_data()       
 st.dataframe(df) 

# EXTRACT and TRANSFORM PAGE
if selected == "SELECT AND STORE":
    tab1,tab2 = st.tabs(["$\huge EXTRACT $", "$\huge TRANSFORM $"])

    # EXTRACT TAB
    with tab1:
        channel_id = st.text_input('**Enter channel_id**')
        if channel_id and st.button("Extract Data"):
                channel_data=get_channel_details(channel_id)
                st.write(channel_data) 
        if st.button("upload to Mongodb"):

                m=channel(channel_id)
                coll.insert_one(m)
                st.success("Upload to Mongodb successful !!")  
    # TRANSFORM TAB 
    with tab2:

        st.header(':green[Migration of Data]')
        st.markdown("#  ")
        st.markdown("select a channel to begin transformation to SQL")
        ch_name=channel_name()
        st.write(ch_name)
        user_inp=st.selectbox("select channel", ch_name)

        if st.button("Submit"):
            m=channel(channel_id)
            sql_main(m)
            st.success("Transformation to MySQL Successful!!!")

# DATA ANALYSIS PAGE
if selected == "DATA ANALYSIS":
    mycursor.execute("USE ytube")
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT video_title, channel_name FROM videodetails ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name, videos_count FROM channeldetails
                            ORDER BY videos_count DESC""")
        df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name, video_title, view_count 
                            FROM videodetails ORDER BY view_count DESC LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT video_title, comment_count from videodetails""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
                    
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name, video_title, likes_count FROM videodetails
                            ORDER BY likes_count DESC LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT video_title, likes_count
                            FROM videodetails
                            ORDER BY likes_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name, channel_views FROM channeldetails
                            ORDER BY channel_views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name, publishedat FROM videodetails                          
                            WHERE year(publishedat)=2022""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name, AVG(duration) FROM videodetails
                            GROUP BY 1""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name, video_id, comment_count FROM videodetails
                            ORDER BY comment_count DESC LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

