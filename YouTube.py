import streamlit as st
import pandas as pd
import mysql.connector as db
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
import re
from datetime import datetime

# Suppress warnings globally
st.set_option('deprecation.showfileUploaderEncoding', False)
st.set_option('deprecation.showPyplotGlobalUse', False)

# Establish database connection
def establish_connection():
    try:
        connection = db.connect(host="localhost", user="root", password="123", database="y_data")
        return connection
    except db.Error as e:
        st.error("Error establishing connection: {}".format(e))
        return None

# Function to create the channels table
def create_channel_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS channels (
                              Channel_Name VARCHAR(100),
                              Channel_Id VARCHAR(100) PRIMARY KEY,
                              Subscribers BIGINT,
                              Views BIGINT,
                              Total_Videos INT,
                              Channel_Description TEXT,
                              Playlist_Id VARCHAR(100)
                          )''')
    except db.Error as e:
        st.error("Error creating channels table: {}".format(e))

# Function to insert channel details into the channels table
def insert_channel_details(connection, channels):
    try:
        cursor = connection.cursor()
        for channel in channels:
            query = '''INSERT IGNORE INTO channels (
                                            Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)'''
            val = (channel["Channel_Name"],
                   channel["Channel_Id"],
                   channel["Subscribers"],
                   channel["Views"],
                   channel["Total_Videos"],
                   channel["Channel_Description"],
                   channel["Playlist_Id"])
            cursor.execute(query, val)
        connection.commit()
    except db.Error as e:
        st.error("Error inserting channel details: {}".format(e))

# Function to retrieve channel details
def get_channel_info(YouTube, Y_ChannelId):
    try:
        req = YouTube.channels().list(
            part="snippet, contentDetails, statistics",
            id=Y_ChannelId
        )
        res = req.execute()
        channel_info = []

        if "items" in res:
            for item in res["items"]:
                info = {
                    "Channel_Name": item["snippet"]["title"],
                    "Channel_Id": item["id"],
                    "Subscribers": item["statistics"]["subscriberCount"],
                    "Views": item["statistics"]["viewCount"],
                    "Total_Videos": item["statistics"]["videoCount"],
                    "Channel_Description": item["snippet"]["description"],
                    "Playlist_Id": item["contentDetails"]["relatedPlaylists"]["uploads"]
                }
                channel_info.append(info)
        else:
            st.warning("No channel found")
        return channel_info
    except Exception as e:
        st.error("Error retrieving channel info: {}".format(e))
        return []

# Function to create the Videos table
def create_videos_table():
    try:
        db_connection = db.connect(host="localhost", user="root", password="123", database="y_data")
        cursor = db_connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS videos (
                              Video_Name VARCHAR(100),
                              Channel_Id VARCHAR(100),
                              Video_Id VARCHAR(100) PRIMARY KEY,
                              Title VARCHAR(255),
                              Description TEXT,
                              Publish_Date TIMESTAMP,
                              Duration VARCHAR(50),
                              Definition VARCHAR(50),
                              Caption BOOLEAN,
                              Views_Count BIGINT,
                              Comments INT,
                              Favorite_Count INT,
                              Like_Count BIGINT,
                              Dislike_Count BIGINT,
                              Tags TEXT,
                              Thumbnails TEXT
                          )''')
        db_connection.close()
    except db.Error as e:
        st.error("Error creating videos table: {}".format(e))

# Function to fetch video details from the database
def fetch_video_details():
    try:
        db_connection = db.connect(host="localhost", user="root", password="123", database="y_data")
        cursor = db_connection.cursor()
        cursor.execute("SELECT * FROM videos")
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        db_connection.close()
        return pd.DataFrame(data, columns=columns)
    except db.Error as e:
        st.error("Error fetching video details: {}".format(e))
        return pd.DataFrame()

# Function to convert YouTube duration format to HH:MM:SS format
def convert_duration(duration):
    matches = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration)
    if matches:
        hours = int(matches.group(1)[:-1]) if matches.group(1) else 0
        minutes = int(matches.group(2)[:-1]) if matches.group(2) else 0
        seconds = int(matches.group(3)[:-1]) if matches.group(3) else 0
        return '{:02d}:{:02d}:{:02d}'.format(hours, minutes, seconds)
    else:
        return '00:00:00'  # Return default duration if no match is found

# Function to retrieve video IDs from a given channel ID
def get_video_ids(YouTube, Y_ChannelId):
    try:
        video_ids = []
        req = YouTube.channels().list(id=Y_ChannelId, part="contentDetails")
        res = req.execute()
        if "items" in res and len(res["items"]) > 0:
            playlist_id = res["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            next_page_token = None

            while True:
                req1 = YouTube.playlistItems().list(
                    part="snippet",
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                ).execute()
                for item in req1["items"]:
                    video_ids.append(item["snippet"]["resourceId"]["videoId"])
                next_page_token = req1.get("nextPageToken")

                if next_page_token is None:
                    break
        else:
            st.warning("No items found")        
        return video_ids
    except Exception as e:
        st.error("Error retrieving video IDs:{}". format(e))
        return []

# Function to retrieve video details based on video IDs
def get_video_info(YouTube, video_ids):
    try:
        video_data = []
        for video_id in video_ids:
            req = YouTube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            ).execute()

            for item in req["items"]:
                video_info = {
                    "Video_Name": item["snippet"]["channelTitle"],
                    "Channel_Id": item["snippet"]["channelId"],
                    "Video_Id": item["id"],
                    "Title": item["snippet"]["title"],
                    "Tags": item["snippet"].get("tags", []),
                    "Thumbnails": item["snippet"]["thumbnails"],
                    "Description": item["snippet"]["description"],
                    "Publish_Date": item["snippet"]["publishedAt"],
                    "Duration": item["contentDetails"]["duration"],
                    "Definition": item["contentDetails"]["definition"],
                    "Caption": item["contentDetails"]["caption"],
                    "Views_Count": item["statistics"].get("viewCount", 0),
                    "Comments": item["statistics"].get("commentCount", 0),
                    "Favorite_Count": item["statistics"].get("favoriteCount", 0),
                    "Like_Count" : item["statistics"].get("likeCount", 0),
                    "Dislike_Count" : item["statistics"].get("dislikeCount", 0)
                }
                video_data.append(video_info)
        return video_data
    except Exception as e:
        st.error("Error retrieving video info: {}".format(e))
        return []

# Function to insert video details into the Videos table
def insert_video_details(videos):
    try:
        db_connection = db.connect(host="localhost", user="root", password="123", database="y_data")
        cursor = db_connection.cursor()

        for video in videos:
            publish_date = datetime.strptime(video["Publish_Date"][:-1], '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            caption_value = 1 if video["Caption"] else 0
            thumbnails_str = json.dumps(video["Thumbnails"])

            # Convert YouTube duration format to HH:MM:SS format
            duration = convert_duration(video["Duration"])

            query = '''INSERT INTO videos (
                                           Video_Name,
                                           Channel_Id,
                                           Video_Id,
                                           Title,
                                           Description,
                                           Publish_Date,
                                           Duration,
                                           Definition,
                                           Caption,
                                           Views_Count,
                                           Comments,
                                           Favorite_Count,
                                           Like_Count,
                                           Dislike_Count,
                                           Tags,
                                           Thumbnails)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            val = (video["Video_Name"],
                   video["Channel_Id"],
                   video["Video_Id"],
                   video["Title"],
                   video["Description"],
                   publish_date,
                   duration,
                   video["Definition"],
                   caption_value,
                   video["Views_Count"],
                   video["Comments"],
                   video["Favorite_Count"],
                   video["Like_Count"],
                   video["Dislike_Count"],
                   ",".join(video["Tags"]),
                   thumbnails_str)
            cursor.execute(query, val)
            db_connection.commit()
        db_connection.close()
    except db.Error as e:
        st.error("Error inserting video details: {}".format(e))

# Function to create the playlists table
def create_playlists_table():
    try:
        db_connection = db.connect(host="localhost", user="root", password="123", database="y_data")
        cursor = db_connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS playlists (
                              Playlist_id VARCHAR(100) PRIMARY KEY,
                              Title VARCHAR(255),
                              Channel_id VARCHAR(100),
                              Channel_Title VARCHAR(100),
                              Published_Date TIMESTAMP,
                              Item_Count INT
                          )''')
        db_connection.close()
    except db.Error as e:
        st.error("Error creating playlists table:", e)

# Function to insert playlist details into the playlists table
def insert_playlist_details(playlists):
    try:
        db_connection = db.connect(host="localhost", user="root", password="123", database="y_data")
        cursor = db_connection.cursor()

        for playlist in playlists:
            publish_date = datetime.strptime(playlist["Published_Date"][:-1], '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')

            query = '''INSERT IGNORE INTO playlists (
                                           Playlist_id,
                                           Title,
                                           Channel_id,
                                           Channel_Title,
                                           Published_Date,
                                           Item_Count)
                       VALUES (%s, %s, %s, %s, %s, %s)'''
            val = (playlist["Playlist_id"],
                   playlist["Title"],
                   playlist["Channel_id"],
                   playlist["Channel_Title"],
                   publish_date,
                   playlist["Item_Count"])
            cursor.execute(query, val)
            db_connection.commit()
        db_connection.close()
    except db.Error as e:
        st.error("Error inserting playlist details: {}".format(str(e)))

# Function to get Playlist Details
def get_playlist_details(YouTube, Channel_id):
    try:
        next_page_token = None
        playlist_data = []
        while True:
            req = YouTube.playlists().list(
                part="snippet, contentDetails",
                channelId=Channel_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            for item in req["items"]:
                df = {
                    "Playlist_id": item["id"],
                    "Title": item["snippet"]["title"],
                    "Channel_id": item["snippet"]["channelId"],
                    "Channel_Title": item["snippet"]["channelTitle"],
                    "Published_Date": item["snippet"]["publishedAt"],
                    "Item_Count": item["contentDetails"]["itemCount"]
                }
                playlist_data.append(df)
            next_page_token = req.get("nextPageToken")      
            if next_page_token is None:
                break
        return playlist_data
    except Exception as e:
        st.error("Error retrieving playlist details: {}".format(e))
        return []

# Function to create the comments table
def create_comments_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS comments (
                              Comment_Id VARCHAR(100) PRIMARY KEY,
                              Video_Id VARCHAR(100),
                              Text_Display TEXT,
                              Author_Name VARCHAR(100),
                              Comment_Date TIMESTAMP)
                          ''')
        connection.commit()
    except db.Error as e:
        st.error("Error creating comments table:", e)

# Function to insert comment details into the comments table
def insert_comment_details(connection, comments):
    try:
        cursor = connection.cursor()
        for comment in comments:
            publish_date = datetime.strptime(comment["Comment_Date"][:-1], '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            query = '''INSERT IGNORE INTO comments (
                                            Comment_Id,
                                            Video_Id,
                                            Text_Display,
                                            Author_Name,
                                            Comment_Date)
                                        VALUES (%s, %s, %s, %s, %s)'''
            val = (comment["Comment_Id"],
                   comment["Video_Id"],
                   comment["Text_Display"],
                   comment["Author_Name"],
                   publish_date)
            cursor.execute(query, val)
        connection.commit()
    except db.Error as e:
        st.error("Error inserting comment details: {}".format(e))

# Function to retrieve comment details
def get_comment_info(YouTube, video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            next_page_token = None
            while True:
                req = YouTube.commentThreads().list(
                    part="snippet, replies",
                    videoId=video_id,
                    maxResults=50,
                    pageToken = next_page_token
                ).execute()

                for item in req["items"]:
                    comment_info = {
                        "Comment_Id": item["id"],
                        "Video_Id": item["snippet"]["videoId"],
                        "Text_Display": item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        "Author_Name": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        "Comment_Date": item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                    }
                    comment_data.append(comment_info)
                next_page_token = req.get("nextPageToken")
                if not next_page_token:
                    break    
    except HttpError as e:
        if e.resp.status == 403 and 'commentsDisabled' in str(e):
            st.error("Comments are disabled for the video with ID: {}".format(video_id))
        else:
            st.error("Error retrieving comment info: {}".format(e))
    except Exception as e:
        st.error("Error retrieving comment info: {}".format(e))
        
    return comment_data

# Function to execute SQL queries and return results in DataFrame format
def execute_query(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    except db.Error as e:
        st.error("Error executing query: {}".format(e))
        return pd.DataFrame()
    
# Function to create the SQL queries tab                 
def sql_queries_tab(connection):
    cursor = connection.cursor()
    st.subheader("SQL Queries")
    questions = [
        "1. What are the names of all the videos and their corresponding channels?",
        "2. Which channels have the most number of videos, and how many videos do they have?",
        "3. What are the top 10 most viewed videos and their respective channels?",
        "4. How many comments were made on each video, and what are their corresponding video names?",
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
        "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
        "7. What is the total number of views for each channel, and what are their corresponding channel names?",
        "8. What are the names of all the channels that have published videos in the year 2022 and 2023?",
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
    ]
    selected_question = st.selectbox("Select a question:", questions)
    
    query_results = []
    if selected_question == questions[0]:
        query = """
                SELECT v.Title AS Video_Title, c.Channel_Name AS Channel_Name 
                FROM videos v
                INNER JOIN channels c ON v.Channel_Id = c.Channel_Id
                """
        cursor.execute(query)
        query_results.append(cursor.fetchall())
        column_names = ["Video_Title", "Channel_Name"]

    elif selected_question == questions[1]:
        query = '''
                SELECT c.Channel_Name, COUNT(*) AS Video_Count
                FROM videos v
                INNER JOIN channels c ON v.Channel_Id = c.Channel_Id
                GROUP BY v.Channel_Id
                ORDER BY Video_Count DESC
                '''
        cursor.execute(query)
        query_results.append(cursor.fetchall())
        column_names = ["Channel_Name", "Video_Count"]

    elif selected_question == questions[2]:
        query = """
                SELECT v.Title AS Video_Name, c.Channel_Name, v.Views_Count
                FROM videos v
                INNER JOIN channels c ON v.Channel_Id = c.Channel_Id
                ORDER BY v.Views_Count DESC
                LIMIT 10
                """
        cursor.execute(query)
        query_results.append(cursor.fetchall())
        column_names = ["Video_Name", "Channel_Name", "Views_Count"]

    elif selected_question == questions[3]:
        query = """
                SELECT v.Title AS Video_Name, COUNT(c.Comment_Id) AS Comment_Count
                FROM videos v
                LEFT JOIN comments c ON v.Video_Id = c.Video_Id
                GROUP BY v.Video_Id
                """
        cursor.execute(query)
        query_results.append(cursor.fetchall())
        column_names = ["Video_Name", "Comment_Count"]

    elif selected_question == questions[4]:
        query = """
                SELECT v.Title AS Video_Name, c.Channel_Name, v.Like_Count
                FROM videos v
                INNER JOIN channels c ON v.Channel_Id = c.Channel_Id
                ORDER BY v.Like_Count DESC
                LIMIT 10
                """
        cursor.execute(query)
        query_results.append(cursor.fetchall())
        column_names = ["Video_Name", "Channel_Name", "Like_Count"]

    elif selected_question == questions[5]:
        query = """
                SELECT v.Title AS Video_Name, SUM(v.Like_Count) AS Total_Likes, SUM(v.Dislike_Count) AS Total_Dislikes
                FROM videos v
                GROUP BY v.Video_Id
                """
        cursor.execute(query)
        query_results.append(cursor.fetchall())
        column_names = ["Video_Name", "Total_Likes", "Total_Dislikes"]

    elif selected_question == questions[6]:
        query = """
                SELECT c.Channel_Name, SUM(v.Views_Count) AS Total_Views
                FROM channels c
                INNER JOIN videos v ON c.Channel_Id = v.Channel_Id
                GROUP BY c.Channel_Id
                """
        cursor.execute(query)
        query_results.append(cursor.fetchall())
        column_names = ["Channel_Name", "Total_Views"]

    elif selected_question == questions[7]:
        query = """
                SELECT DISTINCT c.Channel_Name
                FROM channels c
                INNER JOIN videos v ON c.Channel_Id = v.Channel_Id
                WHERE YEAR(v.Publish_Date) IN (2022, 2023)
                """
        cursor.execute(query)
        query_results.append(cursor.fetchall())
        column_names = ["Channel_Name"]

    elif selected_question == questions[8]:
        query = """
                SELECT c.Channel_Name, SEC_TO_TIME(AVG(TIME_TO_SEC(TIMEDIFF(v.Duration, '00:00:00')))) AS Avg_Duration
                FROM channels c
                INNER JOIN videos v ON c.Channel_Id = v.Channel_Id
                GROUP BY c.Channel_Id
                """
        cursor.execute(query)
        query_results.append(cursor.fetchall())
        column_names = ["Channel_Name", "Avg_Duration"]

    elif selected_question == questions[9]:
        query = """
                SELECT v.Title AS Video_Name, ch.Channel_Name, COUNT(co.Comment_Id) AS Comment_Count
                FROM videos v
                INNER JOIN channels ch ON v.Channel_Id = ch.Channel_Id
                LEFT JOIN comments co ON v.Video_Id = co.Video_Id
                GROUP BY v.Video_Id
                ORDER BY Comment_Count DESC
                LIMIT 10
                """
        cursor.execute(query)
        query_results.append(cursor.fetchall())
        column_names = ["Video_Name", "Channel_Name", "Comment_Count"]

    # Display the result in DataFrame
    if query_results:
        combined_data = [result for result in query_results if result]
        if combined_data:
            df = pd.DataFrame([item for sublist in combined_data for item in sublist], columns=column_names)
            st.write(df)
        else:
            st.warning("No data found for the selected question.")


# Streamlit UI
def main():
    
    st.set_page_config(page_title="YouTube Data Harvesting and Warehousing", layout="wide")
    st.markdown("") 
    current_tab = st.sidebar.radio("Navigation", ["Home", "Technologies Used", "Fetch Details"])
    if current_tab == "Home":
        st.markdown("<h1 style='color: red;font-family: Harlow Solid Italic;'> Project: YouTube Data Harvesting and Warehousing </h1>", unsafe_allow_html=True)
        st.markdown("")  # Add a blank line
        
        gif_path = r"C:\Users\sindh\Downloads\fyFl.gif."
        st.image(gif_path, use_column_width=True)

    elif current_tab == "Technologies Used":
        st.markdown("<h1 style='color: red;font-family: Harlow Solid Italic;'>YouTube Data Harvesting and Warehousing</h1>", unsafe_allow_html=True)
        st.markdown("")
        
        st.header("**Technologies Used**")
        st.subheader("1. Python Scripting")
        st.subheader("2. Data Collection")
        st.subheader("3. API Integration")
        st.subheader("4. Data Management using SQL ")
        st.subheader("5. Streamlit")


    elif current_tab == "Fetch Details":  
        st.markdown("<h1 style='color: red;font-family: Harlow Solid Italic;'>YouTube Data Harvesting and Warehousing</h1>", unsafe_allow_html=True)  
    # Database connection
        connection = establish_connection()
        video_ids = []  # Initialize video_ids
        if connection:
            create_channel_table(connection)
            create_videos_table()
            create_playlists_table()
            create_comments_table(connection)
            st.success("Database connection established and tables created successfully!")

            # YouTube API
            Api_id = "### API Key ###"  
            Api_name = "youtube"
            Api_ver = "v3"
            YouTube = build(Api_name, Api_ver, developerKey=Api_id)
         
            Y_ChannelId = st.text_input("Enter YouTube channel ID")
    
    
        if st.button("Fetch Channel Data"):
            if Y_ChannelId:
                channel_info = get_channel_info(YouTube, Y_ChannelId)
                if channel_info:
                    st.write("Channel Info:")
                    st.write(channel_info)
                    insert_channel_details(connection, channel_info)
                    st.success("Channel details inserted successfully!")
                else:
                    st.warning("No channel details found.")
         
        # Fetch video IDs
        if st.button("Fetch Video Data"):
            if Y_ChannelId:
                video_ids = get_video_ids(YouTube, Y_ChannelId)
                if video_ids:
                    video_info = get_video_info(YouTube, video_ids)
                    if video_info:
                        st.success("Video details fetched successfully!")
                        st.write("Video Details:")
                        st.dataframe(pd.DataFrame(video_info))
                        insert_video_details(video_info)
                    else:
                        st.warning("No video details found.")
                else:
                    st.warning("No video IDs found.")

        if st.button("Fetch Playlist Data"):
            if Y_ChannelId:
                playlist_info = get_playlist_details(YouTube, Y_ChannelId)
                if playlist_info:
                    insert_playlist_details(playlist_info)
                    st.success("Playlist details inserted successfully!")
                    st.write("Playlist Details:")
                    st.dataframe(pd.DataFrame(playlist_info))
                else:
                    st.warning("No playlist details found.")      

        # Fetch comments for videos
        if st.button("Fetch Comment Data"):
            if Y_ChannelId:
                video_ids = get_video_ids(YouTube, Y_ChannelId)
                if video_ids:
                    comment_info = get_comment_info(YouTube, video_ids)
                    if comment_info:
                        insert_comment_details(connection, comment_info)
                        st.success("Comment details inserted successfully!")
                        st.write("Comment Details:")
                        st.dataframe(pd.DataFrame(comment_info))
                    else:
                        st.warning("No comment details found.")  
                          
        st.markdown("<h1 style='color: red;font-family: Harlow Solid Italic;'>Execute SQL Queries</h1>", unsafe_allow_html=True)
        sql_queries_tab(connection)                      

    
if __name__ == "__main__":
    main()
