## Spark task

Build on MACOS using docker-compose 

What is included:
- Pyspark 3.0.2
- postgres
- metabase as BI tool
- minio as ingestion tool ( buckets compateble with s3)

The project is developed using docker-compose
to start do:
```
docker build -t cluster-apache-spark:3.0.2 .
```
After building:
```
docker-compose up --build

to view logs of the docker: 
docker-compose logs -f  

wait until all containers are up ... while take a bit. 

Metabase will take a bit longer to start
```

How to ingest data:
```
chmod +x ingest.sh
./ingest.sh

This will ingest data to minio ( as a data lake ) and then we can use spark to read from minio and write to postgres (clean data)
```

While building all the neccessary jars will be downloaded to establish mongo connection

To execute the task:
```
docker exec -it docker-spark-cluster-spark-master-1 bash

Then do

bin/spark-submit --master spark://spark-master:7077 ../spark-apps/exercise.py

```

or we can do:
```
docker exec pyspark_docker-main-spark-master-1 bin/spark-submit --master spark://spark-master:7077 ../spark-apps/exercise.py
```



All the data will be stored in postgres database 
all datasource can be viewed in metabase (http://localhost:3000) ( if choose to uncomment from docker-compose)

- user_browsing: 
    - timstamp parsed to datetime (eg. 2022-05-30 09:09:22)
    - all the rows transformed to uppercase to indentify any anomalies such as (wrong users, wrong transactions , wrong sessions)
    - all the rows with null values are removed
    - assuming here all sessions start with S and all users start with U
    - users,sessions,timestamp are unique ( how a user should at exact same timestamp be have same session) remove duplication
    - pages staring from 1-10 as valid pages ( as in the task description)

- user_transactions: 
    - timstamp parsed to datetime (eg. 2022-05-30 09:09:22)
    - all the rows transformed to uppercase to indentify any anomalies such as (wrong users, wrong transactions , wrong sessions)
    - all the rows with null values are removed
    - assuming here all sessions start with S and all users start with U
    - users,sessions,timestamp are unique ( how a user should at exact same timestamp be have same session) remove duplication
    - only A or B transactions are valid (as in the task description)
    - a transcation is valid only if a browsing history leads to one. eg. can't have a purchase with a date much earlier than browsing history

- user_journey_to_buy_visited:
    - here we end up with with the of the user in the site. keeping the first row (earliest) in the group by will give us,
      the first page the user visited and the last page the user visited with the purchase
    - From here we can see the minutes spent on the site before the purchase 
    - Taking the assumption here a session cant take longer than 1 day ( 1440 minutes) we can see the number of steps the user took before the purchase

- end_transactions:
    - Following same philosophy as above, we can see the last page the user visited before the purchase. Resulting the page where the user made the purchase
    - time took to make the purchase


`Results`
- Which are the most popular pages of the e-shop: 
Used the session_id to count the number of unique visits per site
```
 select "public"."user_browsing".page,COUNT( DISTINCT "public"."user_browsing".session_id) from user_browsing group by "public"."user_browsing".page
```

- Which are the most visits pages of the e-shop: 
Used the user to count the number of unique visits per site
```
 select "public"."user_browsing".page,COUNT( DISTINCT "public"."user_browsing".user) from user_browsing group by "public"."user_browsing".page
```

- How many transactions are performed on each page, counting the sessions:
```
select page,count(distinct "public"."end_transactions".session_id ) from end_transactions group by page
```

- What is the average time to purchase for a user? Average all the time between the first visit and the purchase for all users
```
select avg("public"."time_spend_per_session".date_diff_min ) from time_spend_per_session
```

Finally a dataset is created showing the start-ending page visited with the average time and steps where a user makes a purchase. 
From there we can create tables showing the avg time when starting from a specific page and ending to a specific page.


`After run spark job`
- Start dash plotly app and attach to same network as the spark app
```
from root folder -> cd viz/
then docker build -t viz_site:1.0 .  
```

```
docker run -p 9002:9000  --network=pyspark_docker-main_default viz_site:1.0  
```
