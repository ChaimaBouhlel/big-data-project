# Big Data Project: Real-Time and Batch Tweet Trends Analysis

## Project Overview

This project is designed to **report and analyze tweet trends** in real-time and over time using both **batch** and **streaming** data processing. It leverages the power of big data tools to provide insightful analysis on trending topics, user interactions, among the shared tweets.

![System Architecture](https://github.com/user-attachments/assets/f2d5dac4-5e69-4327-a5c0-9c7ec1277f8f)

## Data Processing Approaches

This project handles tweet data using two types of processing:

### 1. Batch Processing
- **Use case**: Analyzing historical trends and performing aggregations on large datasets collected over a period of time.
- **Tools**: Hadoop (MapReduce) for distributed processing and querying.
- **Frequency**: Periodically processes accumulated data (e.g., daily, weekly).
  
### 2. Real-Time (Streaming) Processing
- **Use case**: Tracking and reporting tweet trends in real-time, such as identifying emerging topics or popular hashtags as they happen.
- **Tools**: Apache Kafka for ingesting live tweet streams, Apache Spark Streaming for processing data in real time.
- **Frequency**: Continuous data ingestion and analysis for real-time insights in a Power BI dashboard.

## Project Components

1. **Data Ingestion**: Real-time tweet streaming using the Twitter API and batch processing from stored datasets.
2. **Data Processing**:
   - **Batch Mode**: Aggregation and analysis over historical datasets.
   - **Real-Time Mode**: Continuous data streaming and trend detection.
3. **Data Storage**: 
   - Batch data stored in distributed storage systems like HDFS.
   - Real-time processed data stored in NoSQL database: MongoDB.
4. **Visualization**: Trending data is visualized using PowerBI.
   
## How to Run the Project

### Prerequisites

- Hadoop ecosystem (for batch processing)
- Apache Kafka (for streaming data)
- Apache Spark (for data processing)

