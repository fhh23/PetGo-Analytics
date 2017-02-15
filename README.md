Insight Data Engineering Project

Presentation: https://docs.google.com/presentation/d/1jQSnEANvE965hPYleOkKVTyo-Q_p9SCWP2kmCIj-1wY/edit#slide=id.p

# PetGo Analytics

![PetGo Analytics - Pet lovers like you also bought...](res/pulse.jpg)

## Project Links

 * [Slides][slides]
 * [Live Demo][demo]

## Outline

1. [Project Overview](README.md#1-introduction)
2. [The Pipeline](README.md#2-the-pipeline)
 * 2.1 [Data Generation](README.md#21-data-generation)
 * 2.2 [Stream Processing and Data Storage](README.md#22-stream-processing-and-data-storage)
 * 2.3 [Batch Processing and Data Storage](README.md#23-batch-processing-and-data-storage)
 * 2.4 [UI Server](README.md#24-ui-server)
3. [Performance](README.md#3-performance)
4. [Future Work](README.md#4-future-work)
5. [Deployment](README.md#5-deployment)



## 1. Project Overview

Amazon has recently announced their idea for the store of the future, called Amazon Go. This is a store with no cashiers, so if you want to purchase an item you simply take it off the shelf and you are automatically charged for it. If you don't want an item, you put it back on the shelf and are refunded. For my project I simulated a pet store version of Amazon Go, called PetGo.

You can imagine that Amazon would want to have the same features that make their website so popular also in their futuristic store. The two features I decided to implement were:

 * Identify what are the frequently bought items (e.g., top 10% of items purchased)
 * Recommend items that are commonly bought together (e.g., Amazon's "people who bought this item also bought..." feature)

There are two main use cases for these features. First, it offers customers a better shopping experience, as they can learn what items are most popular and can get recommendations about items they may have not known about. Second, it allows the business to offer deals and recommendations to customers while they are shopping based on what is currently in their cart.



## 2. The Pipeline

![PetGo Analytics Pipeline](res/pipeline.jpg)

There is both a Batch and Streaming component to the pipeline. Both batch and streaming use the same data stored in S3. Kafka is used to simulate streaming data. Spark is used for both the batch and streaming processing. The results of the streaming processing are stored in Redis, and the results 

### 2.1 Data Generation

[Source](1.avro-schema)

Data was generated using the Apache BigTop project. Data was generated in the format of one customer transaction per line. Below is an example. INSERT IMAGE. 

After the data was generated, it was uploaded to S3.

### 2.2 Stream Processing and Data Storage

<img align="left" src="res/mock_firehose.jpg" />

[Source](2.mock-firehose)

Stream Processing includes Kafka and Spark Streaming. Uses tdigest algorithm. Stores data in Redis.

<br clear="all" />
### 2.3 Batch Processing and Data Storage

<img align="right" src="res/venturi.jpg" />

[Source](2.venturi)

Batch processing reads data from S3. SON and Apriori algorithm. Stores data in RethinkDB.


<br clear="all" />

### 2.4 UI Server

<img align="right" src="res/uiserver.jpg" />

[Source](4.ui-server)

The UI is built as a tornado web app, with
visualizations built using Highcharts (javascript). This app is served by a Flask web server that uses
Ajax calls to push new data
frames down to the browser clients. The Flask server recieves these
new data frames via Redis and RehtinkDB, with a
registered change listener that performs the previously-mentioned
broadcast. 


<br clear="all" />

## 3. Performance

Coming Soon



## 4. Future Work

<img align="right" src="res/minCut.jpg" />

Coming soon


<br clear="all" />

## 5. Deployment

See the [DEPLOY][deploy] guide (Coming Soon)

Much of the ops work on this project was done using the
[Pegasus][pegasus] deployment and management tool. If you'd like to
run your own Network Pulse cluster, the guide above walks you through
the initial setup.



[demo]: http://www.petgoanalytics.us/
[slides]: goo.gl/FTW14K
[InsightDE]: http://insightdataengineering.com/
[pegasus]: https://github.com/insightdatascience/pegasus
[deploy]: DEPLOY.md
