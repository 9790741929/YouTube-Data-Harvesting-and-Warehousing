# pip install google_api_python_client
# pip install isodate
# pip install mysql_connector_python
# pip install sqlalchemy
# pip install streamlit

from isodate import parse_duration
from googleapiclient.discovery import build
import pandas as pd
from pprint import pprint
import mysql.connector as db
from sqlalchemy import create_engine
import streamlit as st


api_service_name = "youtube"
api_version = "v3"

api_key = 'AIzaSyAUJUNQQBXbNEygOy0TTGDbpQrnPk2_Y5k'
youtube = build('youtube', 'v3', developerKey=api_key)

# Function to get channel Details
def channel_data(c_ids):
    all_data = []
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics,id",
        id=c_ids
    )
    response = request.execute()

    for i in range (len(response['items'])):
        data =  {'channel_name':response['items'][i]['snippet']['title'],
                  'channel_id':response['items'][i]['id'],
                  'channel_Description':response['items'][i]['snippet']['description'],
                  'channel_view_count':response['items'][i]['statistics']['viewCount'] ,
                  'channel_subcript_count':response['items'][i]['statistics']['subscriberCount'] ,
                  'channel_videos_count':response['items'][i]['statistics']['videoCount'],
                  'playlist_id' :response['items'][i]['contentDetails']['relatedPlaylists']['uploads']}
    all_data.append(data)

    return (all_data)

# Function to get all video_ids

def playlist_data(c_ids):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics,id",
        id=c_ids
    )
    response = request.execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    request = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()

    video_ids = []

    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

    next_Page_Token = response.get('nextPageToken')
    more_pages = True

    while more_pages:
       if next_Page_Token is None:
          more_pages = False
       else:
           request = youtube.playlistItems().list(
                        part="contentDetails",
                        playlistId=playlist_id,
                        maxResults=50,
                        pageToken = next_Page_Token
                        )

           response = request.execute()
           for i in range(len(response['items'])):
              video_ids.append(response['items'][i]['contentDetails']['videoId'])

           next_Page_Token = response.get('nextPageToken')

    return video_ids

# Function to get Video Details

def video_data(v_ids):

  All_Video_stats = []

  for i in v_ids:
    request = youtube.videos().list(
        part="snippet,contentDetails,id,statistics",
        id= i)
    response = request.execute()

    for video in response['items']:
      video_stats = {'video_id': video['id'],
                     'channel_id' : video['snippet']['channelId'],
                     'video_name' : video['snippet']['title'],
                     'video_descrip' : video['snippet']['description'],
                     'published_date' : video['snippet']['publishedAt'],
                     'view_count' : video['statistics']['viewCount'],
                     'like_count'  : video['statistics']['likeCount'],
                     'favorite_count' : video['statistics']['favoriteCount'],
                     'comment_count' : video['statistics']['commentCount'],
                     'Duration' : video['contentDetails']['duration'],
                     'thumbnails' : video['snippet']['thumbnails']['default']['url'],
                     'caption_status' : video['contentDetails']['caption']}
      All_Video_stats.append(video_stats)


  return All_Video_stats

# Function to get comment details

def comments_data(v_ids):
  all_comments = []
  try:
    next_page = None
    for i in v_ids:
      request = youtube.commentThreads().list(
                  part="snippet",
                  videoId =i,
                  maxResults =50,
                  pageToken = next_page
        )
      response = request.execute()

      for comments in response['items']:
            comts_data ={'comment_id' :comments['snippet']['topLevelComment']['id'],
                            'video_id' : comments['snippet']['topLevelComment']['snippet']['videoId'],
                            'comment_text' :comments['snippet']['topLevelComment']['snippet']['textOriginal'],
                            'comment_author' :comments['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'comment_date' :comments['snippet']['topLevelComment']['snippet']['publishedAt'] }

            all_comments.append(comts_data)

  except:
       pass
  return all_comments

# **common function for details**
def channel_call(c_ids):
    A = channel_data(c_ids)
    B = playlist_data(c_ids)
    C = video_data(B)
    D= comments_data(B)
    return A,C,D


#  **Connecting to mysql**

db_connection =db.connect( host ="localhost",
                          port = 3306,
                          user = "root",
                           password = "root",
                           database ="youtube_python")
db_connection

curr =db_connection.cursor()


# **  CREATING ENGINE TO SQL MIGRATION**

engine = create_engine("mysql+mysqlconnector://root:root@localhost/youtube_python")

def channel_table(channel_details):
    channel_details.to_sql('ch_data', con=engine, if_exists='append', index=False)
    
   # VIDEO TABLE CREATION
   
def video_table(video_details):
    video_details.to_sql('vd_data', con=engine, if_exists='append', index=False)
    
   # COMMENT TABLE CREATION 
   
def comment_table(comment_details):
    comment_details.to_sql('comm_data', con=engine, if_exists='append', index=False)
    
    # **COMMON FUNCTION FOR TABLES**
    # **creating dataframe**
def tables(A,C,D):
    channel_details= pd.DataFrame(A)
    video_details = pd.DataFrame(C)
    def dur(duration_str):
            duration_seconds = parse_duration(duration_str).total_seconds()
            return duration_seconds
    video_details["dur_secs"]=video_details["Duration"].apply(dur)
    comment_details= pd.DataFrame(D)
    
    channel_table(channel_details)
    video_table(video_details)
    comment_table(comment_details)
    return "tables created successfully"

# show tables
def show_channel():
    curr =db_connection.cursor(buffered=True)
    curr.execute("select * from ch_data")
    db_connection.commit()
    CHANNELS =curr.fetchall()
    c = pd.DataFrame(CHANNELS,columns =["channel_name","channel_id","channel_Description",
                                        "channel_view_count","channel_subcript_count","channel_videos_count","playlist_id"])
    st.write(c)
     
def show_video():
    curr =db_connection.cursor(buffered=True)
    curr.execute("select * from vd_data ")
    db_connection.commit()
    VIDEOS =curr.fetchall()
    v = pd.DataFrame(VIDEOS,columns =["video_id","channel_id","video_name","video_descrip","published_date",
                                    "view_count","like_count","favorite_count","comment_count","Duration",
                                    "thumbnails","caption_status","dur_secs"])

    st.write(v)

def show_comment():
    curr =db_connection.cursor(buffered=True)
    curr.execute("select * from comm_data limit 50")
    db_connection.commit()
    COMMENTS =curr.fetchall()
    co = pd.DataFrame(COMMENTS,columns =["comment_id","video_id","comment_text","comment_author","comment_date"])
    st.write(co)
    
#   ** STREAMLIT PART**

with open(r'c:\Users\Admin\OneDrive\Desktop\python\.vscode\style.css') as f:
    st.markdown(f"""<style>{f.read()}</style>""", unsafe_allow_html=True)
with st.sidebar:
    st.title(" :violet[WELCOME TO MY PROJECT]")
    st.header("skill Take Away" ,divider='rainbow')
    st.caption(":blue[_python Scripting_]")
    st.caption(":blue[_Data Collection_]")
    st.caption(":blue[_API Integration_]")
    st.caption(":blue[_Data Management using Mysql_]")
    st.caption(":blue[_Building Streamlit_]")
     
st.title(" :red[YOUTUBE DATA HARVESTING AND WAREHOUSING] :sunglasses:") 
  
channel_id = st.text_input(":green[ENTER THE Channel ID]")

if st.button(":white[COLLECT DATA]",type="primary"):
    curr =db_connection.cursor(buffered=True)
    curr.execute("select channel_id from ch_data")
    db_connection.commit()
    id =curr.fetchall()
    ids = []
    for i in id:
        ids.append(i[0])
    if channel_id in ids:
        st.success("channel Details of the given channel id exists")
    else:
        A,C,D =  channel_call(channel_id)
        st.success("uploading details....")
    
if st.button(":white[Migrate To SQL]",type="primary"):
    A,C,D =  channel_call(channel_id)
    table =tables(A,C,D)
    st.success(table)

show_table= st.radio(":green[SELECT THE TABLE TO VIEW]",("CHANNELS","VIDEOS","COMMENTS"))

if show_table == "CHANNELS":
   show_channel()
    
elif show_table == "VIDEOS":
     show_video()
    
elif show_table == "COMMENTS":
     show_comment()

# SQL CONNECTION for queries
  
curr =db_connection.cursor(buffered=True)

question = st.selectbox(":green[SELECT YOUR QUESTION]",("1.All the videos and their corresponding channels names",
                                                        "2.Channels have the most number of videos",
                                                        "3.Top 10 most viewed videos",
                                                        "4.Comments were made on each video",
                                                        "5.Videos having the highest number of likes", 
                                                        "6.Total number of likes for each video and their corresponding video names",
                                                        "7.Total number of views for each channel",
                                                        "8.All the channels that have published videos in the year 2022",
                                                        "9.Average duration of all videos in each channel",
                                                        "10.Videos having the highest number of comments"))

if question == "1.All the videos and their corresponding channels names":
    query1 = '''select video_name,channel_name from vd_data
                inner join ch_data where ch_data.channel_id = vd_data.channel_id'''
    curr.execute(query1)
    db_connection.commit()
    t1 =curr.fetchall()
    df1 =pd.DataFrame(t1,columns =["video_name","channel_name"])
    st.write(df1)
    
elif question ==  "2.Channels have the most number of videos":    
    query2 = '''select channel_name,channel_videos_count as no_of_videos  from ch_data order by channel_videos_count desc '''
    curr.execute(query2)
    db_connection.commit()
    t2 =curr.fetchall()
    df2 =pd.DataFrame(t2,columns =["channel_name","no_of_videos"])
    st.write(df2)

elif question =="3.Top 10 most viewed videos":
    query3 = '''select view_count,video_name,channel_name from vd_data 
            inner join ch_data on ch_data.channel_id = vd_data.channel_id 
            where view_count is not null order by view_count desc limit 10'''
    curr.execute(query3)
    db_connection.commit()
    t3 =curr.fetchall()
    df3 =pd.DataFrame(t3,columns =["view_count","channel_name","video_name"])
    st.write(df3)
    
elif question == "4.Comments were made on each video":    
    query4 = '''select comment_count,video_name from vd_data where comment_count is not null'''
    curr.execute(query4)
    db_connection.commit()
    t4 =curr.fetchall()
    df4 =pd.DataFrame(t4,columns =["comment_count","video_name"])
    st.write(df4)
    
elif question == "5.Videos having the highest number of likes":    
    query5 = '''select video_name,channel_name,like_count from vd_data 
                inner join ch_data on ch_data.channel_id = vd_data.channel_id
                where like_count is not null order by like_count desc;'''
    curr.execute(query5)
    db_connection.commit()
    t5 =curr.fetchall()
    df5 =pd.DataFrame(t5,columns =["video_name","channel_name","like_count"])
    st.write(df5)
    
elif question =="6.Total number of likes for each video and their corresponding video names":    
    query6 = '''select like_count,video_name from vd_data'''
    curr.execute(query6)
    db_connection.commit()
    t6 =curr.fetchall()
    df6 =pd.DataFrame(t6,columns =["like_count","video_name"])
    st.write(df6)
    
elif question == "7.Total number of views for each channel":  
    query7 = '''select channel_name,channel_view_count from ch_data'''
    curr.execute(query7)
    db_connection.commit()
    t7 =curr.fetchall()
    df7 =pd.DataFrame(t7,columns =["channel_name","channel_view_count"])
    st.write(df7)   
    
elif question =="8.All the channels that have published videos in the year 2022":   
    query8 = '''select video_name,channel_name,published_date from vd_data
            inner join ch_data on ch_data.channel_id = vd_data.channel_id
            where extract(year from published_date)  =2022'''
    curr.execute(query8)
    db_connection.commit()
    t8 =curr.fetchall()
    df8 =pd.DataFrame(t8,columns =["channel_name","video_name","published_date"])
    st.write(df8)     

elif question =="9.Average duration of all videos in each channel":
    #query9 = ''' create view extra as 
            # select channel_id,AVG(dur_secs) as Avg_duration from vd_data group by channel_id;'''
    #curr.execute(query9)
    #db_connection.commit()

    query10 = '''select c.channel_name,e.channel_id,e.Avg_duration from 
                extra as e inner join ch_data as c on c.channel_id = e.channel_id'''
    curr.execute(query10)
    db_connection.commit()
    t10 =curr.fetchall()
    df10 =pd.DataFrame(t10,columns =["channel_name","channel_id","Avg_duration"])
    st.write(df10)
    
elif question == "10.Videos having the highest number of comments":    
    query11 = '''select video_name,channel_name,comment_count from vd_data 
            inner join ch_data on ch_data.channel_id = vd_data.channel_id
            where comment_count is not null order by comment_count desc limit 50'''
    curr.execute(query11)
    db_connection.commit()
    t11 =curr.fetchall()
    df11 =pd.DataFrame(t11,columns =["video_name","channel_name","comment_count"])
    st.write(df11)
    


    
    
    
    