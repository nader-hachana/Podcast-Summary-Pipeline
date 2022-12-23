# Download Airflow
>sudo pip install "apache-airflow==2.3.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.3.1/constraints-3.9.txt"

# Download pandas for airflow for a later use
>pip install apache-airflow[pandas]

# Create the database on the local directory
sqlite3 episodes.db
sqlite> .databases                  #visualize dbs 
sqlite> .quit

# Run Airflow server
>airflow standalone

# Add a connection
>airflow connections add 'podcasts' --conn-type 'sqlite' --conn-host '/mnt/c/Users/Nader Hachana/OneDrive/Bureau/P/episodes.db'

# Visualize the connection
>airflow connections get podcasts

# Move python file to airflow dags for each modification to the file
>sudo cp podcast_summary.py $AIRFLOW_HOME/dags

# Configure the airflow home variable
>export AIRFLOW_HOME=~/airflow

# Visualize the result on the terminal
>airflow dags show podcast_summary

# Save the Dag graph as an image on the local directory
>sudo apt-get install graphviz
>airflow dags show podcast_summary --save podcast_summary.png







/home/nader/airflow/dags
/mnt/c/Users/Nader Hachana/OneDrive/Documents/Projects/ETL_Pipeline_Airflow

