# YT-DHW
Retrieve details from Youtube

Hello, Greetings of the day

To explain how this Youtube Data Harvesting and Warehousing is prepared and how it can be used.

NOTE:
	This project is all about only by entering youtube channel id retrieving only channel infos,video infos and their comments information, to view it if needed to save it in the database for further use. This will use mainly python,mysql and streamlit app.




A quick demo of "python" coding(back end)

import packages-----API connection-----database-----channel-----video-----comments-----Table Creation-----Streamlit codes.



A quick demo for end user(front end) "streamlit"

Enter channelid-----view channel,video,comment data----option to save-----quick access of some infos by selecting options.



first import all required packages
	1.from googleapiclient.discovery import build--(for API connection)
	2.import pandas as pd--------------------------(to convert into dataframe)
	3.import isodate-------------------------------(to convert ISO datetime to seconds)
	4.import streamlit as st-----------------------(to show in frontend)
	5.import mysql.connector-----------------------(connecting MySQL database)
        6.import ploty.express as px ------------------(for graph visualization)

1.Then to retrieve datas from yt through API key creating function
eg code;

#api connection
def Api_connect():
    Api_ID="API KEY"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_ID)
    return youtube

youtube=Api_connect()

2.connection to mysql database(local)

	#connection for mysql database
	import mysql.connector
	mydb = mysql.connector.connect(host="localhost",user="root",password="",)
	mycursor = mydb.cursor(buffered=True)


3.function to retrive channel data

	# get channels information

4.function to retrive videoids
	we need to retrive videoids from playlistid to get video details.
	
	#getting video ids

5.function to retrive video information using videoids

	#getting video details(without duration)

6.function to retrive all videos duration as from raw data it will as ISO 8601 format (eg.PT XXH YYM ZZS)
	
	#converting ISO to time

7.function to join the duration values into videos data.

	#joining 
	def joining_df(df1,df2):
   		 joined_df=df1.join(df2)
    		 return joined_df

8.function to get all comments details

	#get comment information

9.function to store datas to database (all three tables)
	
	#creating database
	#creating table for channel details
	#creating table for video details
	#creating table for comments

10.Streamlit codes to deploy .


					"Thanks for Reading Me"
