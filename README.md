<h1 align="center">ETL Pipeline with Airflow, MongoDB and PostgreSQL</h1>

<p align="center">
  <a href="#about">About</a> •
  <a href="#scenario">Scenario</a> •
  <a href="#prerequisites">Prerequisites</a> •
  <a href="#set-up">Set-up</a> •
  <a href="#installation">Installation</a> •
  <a href="#airflow-interface">Airflow Interface</a> •
  <a href="#pipeline-task-by-task">Pipeline Task by Task</a> •
  <a href="#learning-resources">Learning Resources</a> •
  <a href="#encountered-difficulty">Encountered difficulty </a> •
  <a href="#next-steps">Next Steps</a> 
</p>

---
## About

Educational project on how to build an ETL (Extract, Transform, Load) data pipeline, orchestrated with Airflow.
 
The data is extracted from Twitter. It is then transformed/processed with Python and loaded/stored in  **MongoDB** and in **PostgreSQl**.

**MongoDB**  is used as a Database in which json files are stored. Before a load, data are cleaned.

**PostgreSQL** is used as Database with two tables : users and tweets. Data are not cleaned.

## Pipeline

<p align="center"><img src=https://github.com/abdellah-idris/images/blob/main/etl.png></p>

## Scenario

As a **Twitter user**, we face the problem of having to **manually** search for tweets related to a specific domain or user.
With a dashboards showing multiple or single tweets related to multiple or single domain will be a significant improvement.

- Pipeline would be run every hour `due to the Tweeter API limit` to extract the latest tweets.

---

## Prerequisites
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)
- [mongoDB Database](https://www.mongodb.com/basics/create-database)
- [PostgreSQL Database](https://www.postgresql.org/)
- [Twitter API](https://developer.twitter.com/en/docs/twitter-api)

---

## Set-up


**Twitter**
    
- Create a Twitter Developer account.

**MongoDB**  

- Register to [**MongoDB Atlas**](https://www.mongodb.com/). 
- Create a Database *etl* and a Collection *News*.
- Ensure that network access is enabled.
    

**PostgreSQL**:

- Create a localhost Database named `twitterETL`.


**Airflow**:

- Setup Variables and connection 

<img src="https://github.com/abdellah-idris/images/blob/main/Airflow%20Variables.png" align="centre">

<img src="https://github.com/abdellah-idris/images/blob/main/postgreSQl.png" align="centre">

- *Schema* : Represent the PostgreSQl Database Name

## Installation
Build the Docker images and start the containers with:

    docker build . --tag extending_airflow:2.5.1
- Refer to the [Airflow Installation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) before next step.

Run Airflow:

    docker-compose up -d


After everything has been installed, you can check the status of your containers (if they are healthy) with:

    docker ps

<img src="https://github.com/abdellah-idris/images/blob/main/docker%20ps.png" align="centre">

**Note**: it might take up to 30 seconds for the containers to have the **healthy** flag after starting.


## Airflow Interface

You can now access the Airflow web interface by going to http://localhost:8080/. If you have not changed them in the docker-compose.yml file, the default user is **airflow** and password is **airflow**:

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/114421290-d5060d80-9bbd-11eb-842e-13a244996200.png></p>

After signing in, the Airflow home page is the DAGs list page. Here you will see all DAGs.


**Note**: If you update the code in the python DAG script, the airflow DAGs page has to be refreshed


<p align="center"><img src=https://github.com/abdellah-idris/images/blob/main/dags.png></p>


You can access a DAG :

- **PostgreSQL**
<p align="center"><img src=https://github.com/abdellah-idris/images/blob/main/psqldag.png></p>


- **MongoDB**
<p align="center"><img src=https://github.com/abdellah-idris/images/blob/main/mongodbdag.png></p>



## Pipeline Task by Task

#### Task `create_postgres_table`

Create PostgreSQl table `users` and `tweets` if it does not exist.

#### Task `clear`

Clear MongoDB collection.

#### Task `extract_transform`

Extract tweets from Twitter API and transform them into a json file, using only needed fields.


TWEET_INFO

- text
- created_at

USER_INFO
- username
- followers_count
- following_count
- created_at
- description

#### Task `transform`


Used in PostgreSQl DAG. Deleting  `'`  character  in the text and description field. 

**Note**: `'` is a reserved character in PostgreSQl.

#### Task `load`

Load the processed data.


- **MongoDB**
<p align="center"><img src=https://github.com/abdellah-idris/images/blob/main/mongoDBcollection.png></p>

- **Postgres**:

    - Users 

<p align="center"><img src=https://github.com/abdellah-idris/images/blob/main/postgresusers.png></p>

    - Tweets

<p align="center"><img src=https://github.com/abdellah-idris/images/blob/main/postgresqldata.png></p>


## Learning Resources

 - [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
 - [Airflow Tutorial for Beginners - Full Course in 2 Hours 2022](https://youtu.be/K9AnJ9_ZAXE) 

## Encountered difficulty 

- Airflow installation and setup
- Scrape data from Amazon website, as the first idea was  to follow prices evolution of a product.

## Next Steps

- Implementing the UI with flask. 
