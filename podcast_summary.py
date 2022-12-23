from airflow.decorators import dag, task
import pendulum

import requests
import xmltodict
import os

from airflow.providers.sqlite.operators.sqlite import SqliteOperator

# Using SqliteHook to easily query and more use Sqlite from python
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

@dag(
    dag_id= 'podcast_summary',
    schedule_interval= '@daily',
    start_date= pendulum.datetime(2022,12,20),
    catchup= False
)

# Create a funtion that contains all the logic in our data pipeline
def podcast_summary():
    
    # Create a task that creates a table called "episodes" in sqlite database "episodes.db"
    create_database = SqliteOperator(
        task_id="create_table_sqlite",
        sql= r"""
        CREATE TABLE IF NOT EXISTS episodes (
            link TEXT PRIMARY KEY,
            title TEXT,
            filename TEXT,
            published TEXT,
            description TEXT
        )
        """,
        sqlite_conn_id="podcasts"
    )

    # Create a task that retreives episodes data from xml content and parse it
    @task()
    def get_episodes():
        data = requests.get("https://marketplace.org/feed/podcast/marketplace")  # the link provides 50 free podcasts information and updates daily
        feed = xmltodict.parse(data.text)                                        # it adds one new podcast and delete the oldest one per day
        episodes = feed['rss']['channel']['item']
        print(f"Found {len(episodes)} episodes.")
        return episodes
    
    # Run the task get_episodes task
    podcast_episodes = get_episodes()

    # Run the task create_database before running the task get_episodes
    create_database.set_downstream(podcast_episodes)

    # Create a task that loades the retreived episodes into the table "episodes"
    @task()
    def load_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id="podcasts")
        stored = hook.get_pandas_df("SELECT * FROM episodes;")
        new_episodes = []
        for episode in episodes:
            if episode["link"] not in stored["link"].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([episode["link"], episode["title"], episode["pubDate"], episode["description"], filename])
        hook.insert_rows(
                    table="episodes",
                    rows=new_episodes,
                    target_fields=["link", "title", "published", "description", "filename"]
        )

    # Run the task load_episodes which depend on get_episodes
    load_episodes(podcast_episodes)

    # Create a task that downloades the episodes aka podcasts from the retreived episodes and loads them into the folder "Episodes"
    @task()
    def download_episodes(episodes):
        for episode in episodes:
            filename = f"{episode['link'].split('/')[-1]}.mp3"
            audio_path = os.path.join("Episodes", filename)
            if not os.path.exists(audio_path):
                print(f"Downloading {filename}")
                audio = requests.get(episode["enclosure"]["@url"])
                with open(audio_path, "wb+") as f:
                    f.write(audio.content)

    # Run the task download_episodes which depend on get_episodes
    download_episodes(podcast_episodes)

# Initialize the DAG aka data pipeline "podcast_summary" in airflow
summary = podcast_summary()

