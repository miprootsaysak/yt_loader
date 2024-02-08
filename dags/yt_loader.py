import pendulum
import logging
from typing import TypedDict
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from sqlalchemy import Table, MetaData, Column, Integer, create_engine, insert, text, VARCHAR, Boolean, DATETIME
from googleapiclient.discovery import build
import pandas as pd
import datetime
import isodate


class TitleData(TypedDict):
    title_id: int
    title: str


class ChannelData(TypedDict):
    yt_channel_id: str
    channel_title: str
    subscribers_count: int
    videos_count: int


class VideoDetailsData(TypedDict):
    yt_video_id: str
    video_title: str
    description: str
    duration: int
    published_at: str


class VideoData(TypedDict):
    channel_id: str
    video_id: str
    view_count: int
    like_count: int
    comment_count: int
    title_id: int


# Metadata for tables in db
def get_tables(db_conn: str) -> dict[str, Table]:
    engine = create_engine(db_conn, implicit_returning=False)
    meta = MetaData()
    title_table = Table(
        'title', meta,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('by_title', VARCHAR(50)),
        Column('is_current', Boolean, default=1),
        Column('is_deleted', Boolean, default=0),
        Column('created_at', DATETIME, default=datetime.datetime.utcnow),
        Column('expired_at', DATETIME)
    )
    channel_table = Table(
        'channel', meta,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('channel_title', VARCHAR(20)),
        Column('yt_channel_id', VARCHAR(24)),
        Column('subscribers_count', Integer),
        Column('videos_count', Integer),
        Column('is_current', Boolean, default=1),
        Column('is_deleted', Boolean, default=0),
        Column('created_at', DATETIME, default=datetime.datetime.utcnow),
        Column('expired_at', DATETIME)
    )
    video_table = Table(
        'video', meta,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('channel_id', VARCHAR(20)),
        Column('video_id', VARCHAR(15)),
        Column('view_count', Integer),
        Column('like_count', Integer),
        Column('comment_count', Integer),
        Column('title_id', Integer),
        Column('is_current', Boolean, default=1),
        Column('is_deleted', Boolean, default=0),
        Column('created_at', DATETIME, default=datetime.datetime.utcnow),
        Column('expired_at', DATETIME)
    )
    video_details_table = Table(
        'video_details', meta,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('yt_video_id', VARCHAR(15)),
        Column('video_title', VARCHAR(150)),
        Column('description', VARCHAR(5000)),
        Column('duration', Integer),
        Column('published_at', DATETIME),
        Column('is_current', Boolean, default=1),
        Column('is_deleted', Boolean, default=0),
        Column('created_at', DATETIME, default=datetime.datetime.utcnow),
        Column('expired_at', DATETIME)
    )

    meta.create_all(engine)
    return {
        'title': title_table,
        'channel': channel_table,
        'video': video_table,
        'video_details': video_details_table
    }


# Data loader from youtube and transform for better usage form
def store_results(title_name: str, title_id: int, apikey: str) -> dict[str, Any]:
    title = []
    channel_id = []
    channel_title = []
    video_id = []
    view_count = []
    like_count = []
    comment_count = []
    subscriber_count = []
    videos_count = []
    publish_time = []
    video_description = []
    duration = []
    # maxResults to provide info about how many videos need to load
    youtube = build('youtube', 'v3', developerKey=apikey)
    response = youtube.search().list(
        q=title_name,
        type="video",
        pageToken=None,
        order="relevance",
        part="id,snippet",
        maxResults=50,
        location=None,
        locationRadius=None
    ).execute()

    for search_result in response.get("items", []):
        if search_result["id"]["kind"] == "youtube#video":
            title.append(search_result['snippet']['title'])
            video_id.append(search_result['id']['videoId'])
            stats = youtube.videos().list(
                part='statistics, snippet, contentDetails',
                id=search_result['id']['videoId']
            ).execute()
            response = youtube.channels().list(
                part='statistics',
                id=stats['items'][0]['snippet']['channelId']
            ).execute()
            subscriber_count.append(int(response['items'][0]['statistics']['subscriberCount']))
            videos_count.append(int(response['items'][0]['statistics']['videoCount']))
            channel_id.append(stats['items'][0]['snippet']['channelId'])
            channel_title.append(stats['items'][0]['snippet']['channelTitle'])
            view_count.append(int(stats['items'][0]['statistics']['viewCount']))
            video_description.append(stats['items'][0]['snippet']['description'][:5000])
            publish_time.append(stats['items'][0]['snippet']['publishedAt'])
            duration.append(
                int((isodate.parse_duration(stats['items'][0]['contentDetails']['duration'])).total_seconds()))

            try:
                like_count.append(int(stats['items'][0]['statistics']['likeCount']))
            except:
                like_count.append(0)

            if 'commentCount' in stats['items'][0]['statistics'].keys():
                comment_count.append(int(stats['items'][0]['statistics']['commentCount']))
            else:
                comment_count.append(0)

    youtube_dict = {
        'channel_id': channel_id,
        'channel_title': channel_title,
        'video_title': title,
        'video_id': video_id,
        'view_count': view_count,
        'like_count': like_count,
        'comment_count': comment_count,
        'video_description': video_description,
        'subscriber_count': subscriber_count,
        'videos_count': videos_count,
        'published_at': publish_time,
        'duration': duration,
        'title_id': [title_id for i in range(len(channel_id))]
    }
    return youtube_dict


@task
def get_titles(db_conn: str) -> list[TitleData]:
    logging.info('Get titles from db')
    title_list = []
    logging.info(f'Get titles from titles table')
    engine = create_engine(db_conn)
    with engine.connect() as connection:
        result = connection.execute(text("select id, by_title from title"))
        for row in result:
            title_list.append({
                'title_id': row.id,
                'title': row.by_title
            })
    return title_list


@task
def get_youtube_data(titles_list: list[TitleData], yt_apikey: str) -> None:
    logging.info('Starting load data from youtube via API')
    channel_data = []
    video_details_data = []
    video_data = []
    for title in titles_list:
        data = store_results(
            title_name=title['title'],
            title_id=title['title_id'],
            apikey=yt_apikey)
        for i in range(len(data['channel_id'])):
            if data['subscriber_count'][i] >= 1000:
                channel_data.append({
                    'yt_channel_id': data['channel_id'][i],
                    'channel_title': data['channel_title'][i],
                    'subscribers_count': data['subscriber_count'][i],
                    'videos_count': data['videos_count'][i]
                })

                video_details_data.append({
                    'yt_video_id': data['video_id'][i],
                    'video_title': data['video_title'][i],
                    'description': data['video_description'][i],
                    'duration': data['duration'][i],
                    'published_at': data['published_at'][i]
                })

                video_data.append({
                    'channel_id': data['channel_id'][i],
                    'video_id': data['video_id'][i],
                    'view_count': data['view_count'][i],
                    'like_count': data['like_count'][i],
                    'comment_count': data['comment_count'][i],
                    'title_id': data['title_id'][i]
                })
    logging.info('Starting load data to local drive (S3 feature)')
    df = pd.DataFrame.from_dict(channel_data)
    df.to_csv("channel_data.csv", sep=';')
    df = pd.DataFrame.from_dict(video_details_data)
    df.to_csv("video_details_data.csv", sep=';')
    df = pd.DataFrame.from_dict(video_data)
    df.to_csv("video_data.csv", sep=';')
    logging.info('Data loaded!')


@task
def write_data_to_channel_table(db_conn: str) -> None:
    logging.info('Starting to load into channel table')
    df = pd.read_csv("channel_data.csv", index_col=0, sep=';')
    channel_data = df.to_dict('records')
    if channel_data:
        engine = create_engine(db_conn, implicit_returning=False)
        channel_table = get_tables(db_conn=db_conn)['channel']

        connection = engine.connect()
        connection.execute(channel_table.insert(), channel_data)
    else:
        logging.warning('No data for load')


@task
def write_data_to_video_details_table(db_conn: str) -> None:
    logging.info('Starting to load into video details table')
    df = pd.read_csv("video_details_data.csv", index_col=0, sep=';')
    video_details_data = df.to_dict('records')
    if video_details_data:
        engine = create_engine(db_conn, implicit_returning=False)
        channel_table = get_tables(db_conn=db_conn)['video_details']

        connection = engine.connect()
        connection.execute(channel_table.insert(), video_details_data)
    else:
        logging.warning('No data for load')


@task
def write_data_to_video_table(db_conn: str) -> None:
    logging.info('Starting to load into video table')
    df = pd.read_csv("video_data.csv", index_col=0, sep=';')
    video_data = df.to_dict('records')
    if video_data:
        engine = create_engine(db_conn, implicit_returning=False)
        channel_table = get_tables(db_conn=db_conn)['video']

        connection = engine.connect()
        connection.execute(channel_table.insert(), video_data)
    else:
        logging.warning('No data for load')


@dag(
    dag_id='yt_loader',
    start_date=pendulum.datetime(2024, 2, 1),
    schedule=None,
    catchup=False,
    tags=['load', 'youtube', 'ms_sql'],
    is_paused_upon_creation=False,
    params={},
)
def pipeline():
    yt_apikey = Variable.get("yt_apikey")
    ms_sql_conn_string = Variable.get("ms_sql_conn_string")
    titles = get_titles(db_conn=ms_sql_conn_string)
    youtube_data_operator = get_youtube_data(titles_list=titles, yt_apikey=yt_apikey)
    write_video_table_operator = write_data_to_video_table(db_conn=ms_sql_conn_string)
    youtube_data_operator >> write_data_to_channel_table(db_conn=ms_sql_conn_string) >> write_video_table_operator
    youtube_data_operator >> write_data_to_video_details_table(db_conn=ms_sql_conn_string) >> write_video_table_operator


pipeline()
