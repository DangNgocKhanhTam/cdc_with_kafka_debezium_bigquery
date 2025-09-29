##  1. High level architecture ## 
In order to implement real-time CDC-based change replication from MySQL to Bigquery, we will implement the following architecture

<img width="480" height="660" alt="image" src="https://github.com/user-attachments/assets/f26522f5-9c50-45bf-af4c-275bcd32d3b1" />

The Bigquery Sink connector streams CDC events stored in Kafka topic and sent by Debezium, automatically transforms events to the DML SQL statements (INSERT / UPDATE / DELETE), and executes SQL statements in the target database in the order they were created

## 2.Setup required services ## 
### 1. Let’s start with creating a new directory. Open Terminal and run: ### 
<pre> $ mkdir kafka-connect-tuto && cd kafka-connect-tuto </pre>

### 2. Create a plugins directory: ### 

<pre> $ mkdir plugins </pre>

### 3. Download Debezium mysql plugin: ### 

<pre> $ wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/2.1.1.Final/debezium-connector-mysql-2.1.1.Final-plugin.tar.gz -O mysql-plugin.tar.gz </pre>

<pre> $ tar -xzf mysql-plugin.tar.gz -C plugins </pre>

### 4. Download BigQuery plugin ## 

 Download BigQuery plugin and put the contents into your plugins directory (in this tutorial we are using version v2.4.3). Now your plugins directory should look like this:

<pre> $ ls plugins </pre>

<pre> debezium-connector-mysql		wepay-kafka-connect-bigquery-2.4.3  </pre>

In order to setup kafka and zookeeper, we can either use the Kafka server available in GCP marketplace or use docker-compose. In this article, I will go with the second option for cost purpose. 

Below our docker-compose file to deploy all the required services locally

<pre> version: '2'
services:
  zookeeper:
    container_name: zookeeper
    image: quay.io/debezium/zookeeper:2.1
    ports:
     - 2181:2181
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes
  kafka:
    container_name: kafka
    image: quay.io/debezium/kafka:2.1
    ports:
     - 9092:9092
    links:
     - zookeeper
    environment:
     - ZOOKEEPER_CONNECT=zookeeper:2181
  mysql:
    container_name: mysql
    image: quay.io/debezium/example-mysql:2.1
    ports:
     - 3306:3306
    environment:
     - MYSQL_ROOT_PASSWORD=debezium
     - MYSQL_USER=mysqluser
     - MYSQL_PASSWORD=mysqlpw
  connect:
    container_name: connect
    image: quay.io/debezium/connect-base:2.1
    volumes:
      - ./plugins:/kafka/connect
    ports:
     - 8083:8083
    links:
     - kafka
     - mysql
    environment:
     - BOOTSTRAP_SERVERS=kafka:9092
     - GROUP_ID=1
     - CONFIG_STORAGE_TOPIC=my_connect_configs
     - OFFSET_STORAGE_TOPIC=my_connect_offsets
     - STATUS_STORAGE_TOPIC=my_connect_statuses
  debezium-ui:
    image: quay.io/debezium/debezium-ui:2.2
    ports:
     - 8090:8080
    links:
     - connect
    environment:
     - KAFKA_CONNECT_URIS=http://connect:8083</pre>

Let’s explain the different components:

+ We first create a zookeeper service with basic configuration. In this, I kept ALLOW_ANONYMOUS_LOGIN environment variable as yes to connect with unauthorized users. Click on zookeeper configuration for more details.
+ Then we have to create a Kafka service. ZOOKEEPER_CONNECT is used to access zookeeper service from Kafka. As we are using docker-compose you can give service name and expose the port of zookeeper container directly. E.g zookeeper:2181
+ Deploy a MySQL database server that includes an example inventory database that includes several tables that are pre-populated with data. The Debezium MySQL connector will capture changes that occur in the sample tables and transmit the change event records to an Apache Kafka topic.
+ After creating Kafka service and our database server, we need to create Kafka Connect distributed service. It will help in importing/exporting data to Kafka and runs connectors to implement custom logic for interacting with an external system.
+ Debezium UI: it allows users to set up and operate connectors more easily using a web interface.

### 6. Let’s start the services: ### 

After runningdocker-compose up , we will have our debezium UI available locally through the port 8090, but without any connector.

<img width="500" height="364" alt="image" src="https://github.com/user-attachments/assets/cd98a600-b7e1-4950-b766-e1138d28a13d" />

You can check if Debezium is running with Kafka Connect API : 

<pre> $ curl -i -X GET -H "Accept:application/json" localhost:8083/connectors </pre>

<img width="500" height="159" alt="image" src="https://github.com/user-attachments/assets/974209a8-5cb0-407e-90e8-4a45d135c7fd" />

We can also verify whether MySQL is running with the example database “inventory”. You can check for the available tables by running: 

<pre> docker exec -it mysql mysql -uroot -pdebezium -D inventory -e "SHOW TABLES;" </pre>

<img width="500" height="435" alt="image" src="https://github.com/user-attachments/assets/dfb04ba6-3229-49e1-9a55-27e373d164df" />

You can perform a query to a table to see its content: 

<pre> $ docker exec -it mysql mysql -uroot -pdebezium -D inventory -e "SELECT * FROM customers;" </pre>

<img width="500" height="277" alt="image" src="https://github.com/user-attachments/assets/248b0733-efb3-4297-a5b3-2a70d065a794" />

## 3. Configure Debezium to start syncing MySQL to Kafka ## 

MySQL has a binary log (binlog) that records all operations in the order in which they are committed to the database. This includes changes to table schemas as well as changes to the data in tables. MySQL uses the binlog for replication and recovery. The Debezium MySQL connector reads the binlog, produces change events for row-level INSERT, UPDATE, and DELETE operations, and emits the change events to Kafka topics 

## 1. Create a new file (“mysql-connector.json”) with these configurations: ## 

<pre> {
 "name": "mysql-connector",
 "config": {
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "tasks.max": "1",
  "database.hostname": "mysql",
  "database.port": "3306",
  "database.user": "root",
  "database.password": "debezium",
  "database.server.id": "184054",
  "topic.prefix": "debezium",
  "database.include.list": "inventory",
  "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
  "schema.history.internal.kafka.topic": "schemahistory.inventory"
 }
}</pre>

As you saw, we need to provide a name for our connector, its class (io.debezium.connector.mysql.MySqlConnector), specify the connection parameters to our database, and finally, specify which database to include in our CDC process (in this case, 'inventory'). Additionally, we have the option to watch/monitor only a specific table instead of the entire database by using the attribute 'table.include.list'. 

To register the connector, run the following command : 

<pre> curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @mysql-connector.json </pre>

<img width="500" height="129" alt="image" src="https://github.com/user-attachments/assets/b11f29fc-57d1-4a8f-b56d-9efe6758a1e7" />

You can also navigate to the UI to check that connector is created: 

<img width="500" height="266" alt="image" src="https://github.com/user-attachments/assets/48095ece-0a44-45f0-be9d-e64fb69cc9be" />

Debezium stores CDC events in a separate topic for each table. For example, the CDC events for the table customers will be stored in a Kafka topic database_server_name.inventory.customers. 

To check the topics created by Debezium connector, run : 

<pre> docker exec -it kafka bash bin/kafka-topics.sh --list  --bootstrap-server kafka:9092 </pre>

<img width="500" height="168" alt="image" src="https://github.com/user-attachments/assets/2c135edf-f01d-479c-a358-360d4e4966b9" />

Now let’s check the debezium CDC events on a specific table : 

<pre>  docker exec -it kafka bash bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic debezium.inventory.addresses --from-beginning </pre>

<img width="500" height="414" alt="image" src="https://github.com/user-attachments/assets/55d85f1e-29fe-4925-9faf-4a05afa59d4d" />

We see a list of dictionaries containing all the operations performed on a specific table. The most important attribute to check is “op”. It’s a mandatory string that describes the type of operation. In an update event value, the op field value is u, signifying that this row changed because of an update, c for create, t for truncate and d for delete. 

<img width="580" height="1522" alt="image" src="https://github.com/user-attachments/assets/05f74c0b-0ec8-44db-983b-615705ceb2ec" />

For more information on Debezium events, see this Debezium documentation. 

### 4. Syncing data to Google BigQuery ### 
Now, we will register a Kafka connector to sink data based on the events streamed into the previously discussed Kafka topics. We will achieve this by using a JSON configuration file named “bigquery-connector.json’”: 

<pre> {
 "name": "inventory-connector-bigquery",
 "config": {
        "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
  "tasks.max": "1",
  "consumer.auto.offset.reset": "earliest",
  "topics.regex": "debezium.inventory.*",
  "sanitizeTopics": "true",
  "autoCreateTables": "true",
  "keyfile": "/bigquery-keyfile.json",
  "schemaRetriever": "com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever",
  "project": "my-gcp-project-id",
  "defaultDataset": "kafka_dataset",
  "allBQFieldsNullable": true,
  "allowNewBigQueryFields": true,
  "transforms": "regexTopicRename,extractAfterData",
  "transforms.regexTopicRename.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.regexTopicRename.regex": "debezium.inventory.(.*)",
  "transforms.regexTopicRename.replacement": "$1",
  "transforms.extractAfterData.type": "io.debezium.transforms.ExtractNewRecordState"
 }
}</pre>

Make sure to replace the dataset “defaultDataset” with the name of the desired dataset in your Bigquery project. If your remove this field, Kafka connector will keep the same name of the source database. You need also to provide a service account key to the connector with Bigquery/DataEditor role on the target dataset. If your kafka connect is deployed in kubernetes or a compute engine, you can remove the attribute “keyfile” and use directly workload identity. 

Before submitting the connector, I will create a empty dataset in bigquery : 

<img width="500" height="103" alt="image" src="https://github.com/user-attachments/assets/f1015595-4779-464d-9af9-5f3a42aacf28" />

Now let’s register our Bigquery sink: 

<pre> curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @bigquery-connector.json </pre>

<img width="500" height="224" alt="image" src="https://github.com/user-attachments/assets/3e983562-0e81-4292-b082-6b11331f27de" />

Just after a few seconds, our SQL database is replicated to BigQuery, great 🎉 !!

<img width="538" height="406" alt="image" src="https://github.com/user-attachments/assets/4dccfd47-787a-4e9a-946f-eb5575fc812c" />

Now, let’s explore the power of CDC !

Our “customers” table contains initially 4 rows in MySQL and Bigquery:

<img width="500" height="438" alt="image" src="https://github.com/user-attachments/assets/83bebe47-c14a-435f-93f3-98d0034796b7" />

Let’s add a new record to the source table :

<pre>docker exec -it mysql mysql -uroot -pdebezium -D inventory -e "INSERT INTO customers VALUES(1111, \"kafka_product\", \"This is Mohamed from kafka connect\",\"mohamed@kafkaconnect.com\");"</pre>

I see the update in MySQL table: 

<img width="500" height="327" alt="image" src="https://github.com/user-attachments/assets/90809180-7c85-41d4-9f42-d3ce82f1475e" />

After just one second, you will see that a new entry has automatically synced to BigQuery :

<img width="500" height="520" alt="image" src="https://github.com/user-attachments/assets/e57d87fd-0d26-4952-9eaa-f00aa01dc8eb" />


















  
