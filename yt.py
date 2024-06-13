from googleapiclient.discovery import build
import pandas as pd
import isodate
import streamlit as st
import plotly.express as px

#api connection
def Api_connect():
    Api_ID="api key"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_ID)
    return youtube

youtube=Api_connect()

#connection for mysql database
import mysql.connector
mydb = mysql.connector.connect(host="localhost",user="root",password="",)
mycursor = mydb.cursor(buffered=True)

# get channels information
def get_channel_info(channelid):
    channel_details=[]
    request1=youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channelid
    )
    response1 = request1.execute()
    if "items" in response1:
        for i in response1["items"]:
            data=dict(channel_name=i["snippet"]["title"],
                    channel_id=i["id"],
                    channel_description=i["snippet"]["description"],
                    channel_published=i["snippet"]["publishedAt"],
                    channel_uploads=i["contentDetails"]["relatedPlaylists"]["uploads"],
                    viewcount=i["statistics"]["viewCount"],
                    subscribcount=i["statistics"]["subscriberCount"],
                    videocount=i["statistics"]["videoCount"])
            channel_details.append(data)
    CD=pd.DataFrame(channel_details)
    return CD

#getting video ids
def get_video_id(channelid):
    Allvideoids=[]
    request2=youtube.channels().list(
            part="contentDetails",
            id=channelid
        )
    response2 = request2.execute()
    if "items" in response2:
        video_ids=response2["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        next_page_token=None

        while True:
            request3=youtube.playlistItems().list(
                    part="snippet",
                    playlistId=video_ids,
                    maxResults=50,pageToken=next_page_token)
            response3=request3.execute()

            for j in range(len(response3["items"])):
                Allvideoids.append(response3["items"][j]["snippet"]["resourceId"]["videoId"])
            
            next_page_token=response3.get('nextPageToken')

            if next_page_token is None:
                    break
        
    return Allvideoids
    
#getting video details
def get_video_info(videocount):
        video_data=[]
        for video_id in videocount:
            request4=youtube.videos().list(
                            part="snippet,contentDetails,statistics",
                            id=video_id
            )
            response4=request4.execute()

            for item in response4["items"]:
                data=dict(Channel_Name=item['snippet']['channelTitle'],
                        channel_Id=item['snippet']['channelId'],
                        video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet']['description'],
                        Published_Date=item['snippet']['publishedAt'],
                        Views=item['statistics']['viewCount'],
                        Likes=item['statistics'].get('likeCount'),
                        comments=item['statistics'].get('commentCount'),
                        Favorite_count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption=item['contentDetails']['caption']
                        )
                video_data.append(data)
        VD=pd.DataFrame(video_data)
        return VD

#converting ISO to time
def convert_iso_time(vid):
    Duration_time=[]
    for b in vid:
        requestm=youtube.videos().list(
                                    part="snippet,contentDetails,statistics",
                                    id=b
                    )
        responsem=requestm.execute()
        for n in responsem["items"]:
            duration_str=n["contentDetails"]["duration"]
            duration=isodate.parse_duration(duration_str) #iso to python timedelta
            total_seconds=int(duration.total_seconds()) #to convert into seconds
            Duration_time.append(total_seconds)
    df=pd.DataFrame(Duration_time,columns=['Duration'])
    return df

#joining 
def joining_df(df1,df2):
    joined_df=df1.join(df2)
    return joined_df

#get comment information
def get_comment_info(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            request5=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id ,
                maxResults=100
            )
            response5=request5.execute()

            for item in response5['items']:
                data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                        video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_publishedat=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)
    except:
        pass
    COMMENT=pd.DataFrame(comment_data)
    return COMMENT

#inserting details into mysql database
def insert_into_database(cha,vide,comm):
    #creating database
    mycursor.execute("create database if not exists YT ")
    mycursor.execute("use YT")
    #creating table for channel details
    try:
        mycursor.execute('''create table if not exists CHANNEL_INFOS(channel_name varchar(225),
                                    channel_id varchar(225) primary key,channel_description Text,
                                    channel_published timestamp, channel_uploads varchar(225),
                                    viewcount int,subscribcount int,videocount int)''')

        query1='''insert into channel_infos(channel_name,channel_id,channel_description,channel_published,
                                    channel_uploads,viewcount,subscribcount,videocount)
                                    values(%s,%s,%s,%s,%s,%s,%s,%s)'''
        send1=cha.values.tolist()
        for row1 in send1:
            mycursor.execute(query1,row1)
        mydb.commit()

    #creating table for video details
        mycursor.execute('''create table if not exists VIDEO_INFOS(Channel_Name varchar(255),
                                    channel_Id varchar(200),video_Id varchar(200) primary key,Title varchar(255),
                                    Thumbnail Text,Description Text,Published_Date timestamp,
                                    Views int,Likes int,comments int,Favorite_count int,
                                    Definition varchar(10),Caption varchar(10),Duration int)''')
        query2='''insert into VIDEO_INFOS(Channel_Name,channel_Id,video_Id,Title,Thumbnail,Description,
                                    Published_Date,Views,Likes,comments,Favorite_count,Definition,
                                    Caption,Duration)
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        send2=vide.values.tolist()
        for row2 in send2: 
            mycursor.execute(query2,row2)
        mydb.commit()
    
    #creating table for comments
        mycursor.execute('''create table if not exists comment_infos(comment_id varchar(200) primary key,video_id varchar(30),
                                comment_text text,comment_author varchar(200), comment_publishedat timestamp)''')
        query3='''insert into comment_infos(comment_id,video_id,comment_text,comment_author,comment_publishedat)
                                    values(%s,%s,%s,%s,%s)'''
        send3=comm.values.tolist()
        for row3 in send3:
            mycursor.execute(query3,row3)
        mydb.commit()
        st.success("Success")
    except:
        st.write("Details already uploaded") 
    return 

def update(a,b):
    mycursor.execute("use yt")
    
    querya='''delete from channel_infos where channel_id=%s'''
    dela=(a,)
    mycursor.execute(querya,dela)
    mydb.commit()

    queryb='''delete from video_infos where channel_id=%s'''
    delb=(a,)
    mycursor.execute(queryb,delb)
    mydb.commit()

    
    mycursor.execute("use yt")
    queryc="delete from comment_infos where video_id in (%s)"
    delc=b
    for i in delc:
        mycursor.execute(queryc,(i,))
        mydb.commit()

    return st.markdown(":sunglasses:")
    

st.set_page_config(page_title="YouTube DHW",
                   layout="wide",
                   page_icon="C:/Users/DELL XPS/Downloads/yt.png")
col1,col2=st.columns([1,4])
with col1:
    st.image("C:/Logo_of_YouTube_(2015-2017).svg.png",width=200)
with col2:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    YTID=st.text_input("Enter the Channel ID")
    channel=get_channel_info(YTID)
    videoid=get_video_id(YTID)
    except_duration=get_video_info(videoid)
    Duration_yt=convert_iso_time(videoid)
    videosdata=joining_df(except_duration,Duration_yt)
    comments=get_comment_info(videoid)

col1,col2=st.columns([1,4])
with col1:
    show_table=st.radio("Select to view",("CLICK","CHANNEL","VIDEOS","COMMENTS"))
with col2:
    st.write("Click Button to Upload Details into Database")
    if st.button("Upload to MySQL Database"):
        insert_into_database(channel,videosdata,comments)
    
    on=st.toggle("Activate Featuer to update with existing data")
    if on:
        tab1,tab2=st.tabs(["Update","Delete"])
        with tab1:
            st.write("Click Button to update with existing data")
            if st.button("Update Existing"):
                update(YTID,videoid)
                insert_into_database(channel,videosdata,comments)

        with tab2:
            st.write("Click Button if need to delete this channel id details if it is stored")
            if st.button("Delete ID Details"):
                update(YTID,videoid)
                st.write("Successfully Deleted")
    
    if show_table=="CLICK":
        st.markdown("**1. Please enter the channel id that you want to Retrieve.**")
        st.markdown("**2. After entering channel id you can view the details by selecting CHANNEL,VIDEOS,COMMENTS on your left.**")
        st.markdown('**3. If you want to store it, Please click the "Upload to MySQL Database" option.**')
        st.header('*"GOOD LUCK"*')

    if show_table=="CHANNEL":
        aaa=channel.to_dict(orient='records')
        st.header("Details of Channel for given Channel Id")
        st.dataframe(aaa)

    elif show_table=="VIDEOS":
        st.header("Details of Videos for given Channel Id")
        bbb=videosdata.to_dict(orient='records')
        st.dataframe(bbb)

    elif show_table=="COMMENTS":
        ccc=comments.to_dict(orient='records')
        st.header("Details of Comments for given Channel Id")
        st.dataframe(ccc)
    else:
        pass

#mysql connection to see details
mycursor.execute("use yt")
st.markdown("**Here are some Important Insights**")
question1=st.selectbox("Select to Display",
                    ("SELECT",
                        "All Channels and their Id Already Stored",
                        "1.What are the names of all the videos and their corresponding channels?",
                        "2.Which channels have the most number of videos, and how many videos do they have?",
                        "3.What are the top 10 most viewed videos and their respective channels?",
                        "4.How many comments were made on each video, and what are their corresponding video names?",
                        "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                        "6.What is the total number of likes for each video, and what are their corresponding video names?",
                        "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                        "8.What are the names of all the channels that have published videos in the year 2022?",
                        "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                        "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question1=="SELECT":
    st.header("Please Select from the above dropbox Queries to Show")

if question1=="All Channels and their Id Already Stored":
    q='''select channel_name,channel_id from channel_infos order by channel_name'''
    mycursor.execute(q)
    mydb.commit()
    t=mycursor.fetchall()
    df=pd.DataFrame(t,columns=["Channel Name","ID"])
    df.index+=1
    st.dataframe(df)

if question1=="1.What are the names of all the videos and their corresponding channels?":
    
    q1='''select Channel_Name,title from video_infos order by Channel_Name asc'''
    mycursor.execute(q1)
    mydb.commit()
    t1=mycursor.fetchall()
    df1=pd.DataFrame(t1,columns=["channel name","video title"])
    show1=df1.to_dict(orient='records')
    st.dataframe(show1)
    

if question1=="2.Which channels have the most number of videos, and how many videos do they have?":

    q2='''select Channel_Name,videocount from channel_infos order by videocount desc'''
    mycursor.execute(q2)
    mydb.commit()
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=["Channel Name","Videos Count"])
    show2=df2.to_dict(orient='records')
    col1,col2=st.columns([2,5])
    with col1:
        st.dataframe(show2)
    with col2:
        fig2=px.bar(df2,x="Channel Name",y="Videos Count",color_discrete_sequence=px.colors.sequential.Redor_r,
                    hover_name="Videos Count")
        st.plotly_chart(fig2)

if question1=="3.What are the top 10 most viewed videos and their respective channels?":

    q3='''select Channel_Name,Title,Views from video_infos order by Views desc limit 10'''
    mycursor.execute(q3)
    mydb.commit()
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=["Channel Name","Videos Name","Total Views"])
    show3=df3.to_dict(orient='records')
    st.dataframe(show3)
    fig3=px.bar(df3,x="Total Views",y="Videos Name",hover_name="Channel Name")
    st.plotly_chart(fig3)

if question1=="4.How many comments were made on each video, and what are their corresponding video names?":

    q4='''select Channel_Name,Title,comments from video_infos order by Channel_Name'''
    mycursor.execute(q4)
    mydb.commit()
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=["Channel Name","Videos Name","Total Comments"])
    show4=df4.to_dict(orient='records')
    st.dataframe(show4)
    fig4=px.bar(df4,x="Total Comments",y="Channel Name",hover_name="Total Comments",color_discrete_sequence=px.colors.sequential.Rainbow_r,
                height=600)
    st.plotly_chart(fig4)

if question1=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    #SELECT Channel_Name,sum(Likes) from video_infos group by Channel_Name order by sum(Likes) DESC
    q5='''select Channel_Name,Title,Likes from video_infos order by Likes desc'''
    mycursor.execute(q5)
    mydb.commit()
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=["Channel Name","Videos Name","Total Likes"])
    show5=df5.to_dict(orient='records')
    st.dataframe(show5)
    fig5=px.pie(data_frame=df5,names="Channel Name",values="Total Likes",hover_data="Videos Name",
                title="Total Like counts for each Channel",hover_name="Total Likes")
    st.plotly_chart(fig5)

if question1=="6.What is the total number of likes for each video, and what are their corresponding video names?":

    q6='''select Channel_Name,Title,Likes from video_infos order by Channel_Name'''
    mycursor.execute(q6)
    mydb.commit()
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=["Channel Name","videos Name","Total Likes"])
    show6=df6.to_dict(orient='records')
    st.dataframe(show6)

if question1=="7.What is the total number of views for each channel, and what are their corresponding channel names?":

    q7='''select Channel_Name,viewcount from channel_infos order by Channel_Name'''
    mycursor.execute(q7)
    mydb.commit()
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=["Channel Name","Total Views"])
    show7=df7.to_dict(orient='records')
    col1,col2=st.columns([2,5])
    with col1:
        st.dataframe(show7)
    with col2:
        fig7=px.pie(data_frame=df7,names="Channel Name",values="Total Views",hover_name="Total Views",
                    title="Total Number of Views")
        st.plotly_chart(fig7)

if question1=="8.What are the names of all the channels that have published videos in the year 2022?":

    q8='''select Channel_Name,Title,Published_Date from video_infos where year(Published_Date) = 2022 order by Channel_Name '''
    mycursor.execute(q8)
    mydb.commit()
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Channel Name","Video Name","Year 2022"])
    show8=df8.to_dict(orient='records')
    st.dataframe(show8)

if question1=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":

    q9='''select Channel_Name,avg(Duration)/60 from video_infos group by Channel_Name'''
    mycursor.execute(q9)
    mydb.commit()
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=["Channel Name","Avg Duration in Minutes"])
    show9=df9.to_dict(orient='records')
    col1,col2=st.columns([2,5])
    with col1:
        st.dataframe(show9)
    with col2:
        fig9=px.line(df9,x="Channel Name",y="Avg Duration in Minutes",hover_name="Avg Duration in Minutes")
        st.plotly_chart(fig9)

if question1=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":

    q10='''select Channel_Name,Title,comments as comments from video_infos where comments is not null
            order by comments desc'''
    mycursor.execute(q10)
    mydb.commit()
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=["Channel Name","Video Name","Comments"])
    show10=df10.to_dict(orient='records')
    st.dataframe(show10)
    fig10=px.bar(df10,x="Channel Name",y="Comments",color_discrete_sequence=px.colors.sequential.Rainbow,
                 hover_name="Comments",hover_data="Video Name")
    st.plotly_chart(fig10)


col1,col2=st.columns([1,4])
with col1:
    if st.button("Click here Before Leave"):
        with col2:
            st.header('*To be continued.....*')
            st.header("*Thanks for now will see soon with new possibilities*")
            
            
#To be Continued................
