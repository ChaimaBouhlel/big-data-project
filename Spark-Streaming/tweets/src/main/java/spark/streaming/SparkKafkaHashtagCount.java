package spark.streaming;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.sum;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class SparkKafkaHashtagCount {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: SparkKafkaHashtagCount <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkKafkaHashtagCount")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topics)
                .option("kafka.group.id", groupId)
                .load();
        df.selectExpr("CAST(value AS STRING)").writeStream().format("console").start();
        // Define the schema for the JSON data
        StructType schema = new StructType()
                .add("content", DataTypes.StringType)
                .add("hashtags", DataTypes.StringType)
                .add("likeCount", DataTypes.IntegerType)
                .add("quoteCount", DataTypes.IntegerType)
                .add("replyCount", DataTypes.IntegerType)
                .add("retweetCount", DataTypes.IntegerType)
                .add("sourceLabel", DataTypes.StringType)
                .add("username", DataTypes.StringType);

        // Parse the JSON data
        Dataset<Row> jsonData = df.selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), schema).as("data"))
                .select("data.*");

        // Calculate the count of each hashtag
        Dataset<Row> hashtagCounts = jsonData
                .groupBy("hashtags")
                .count()
                .sort(desc("count"));

        // Calculate the count of tweets for each user
        Dataset<Row> userTweets = jsonData
                .groupBy("username")
                .count()
                .sort(desc("count"));

        // Calculate the sum of likes for each content
        Dataset<Row> contentLikes = jsonData
                .groupBy("content")  // change this line
                .agg(sum("likeCount").as("totalLikes"))
                .sort(desc("totalLikes"));
        // Define MongoDB connection options
        String uri = "mongodb://mongodb-container:27017";
        String mongoCollection = "tweets_stream";

      /*               hashtagCounts.writeStream().outputMode("complete")
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.write()
                            .format("mongo")
                            .option("uri", mongoURI)
                            .option("database", "tweets_db_stream")
                            .option("collection", mongoCollection)
                            .mode("append")
                            .save();
                }).start(); */

        ConnectionString mongoURI = new ConnectionString(uri);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(mongoURI)
                .build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase("tweets_db_stream");
        MongoCollection<Document> collection = database.getCollection("tweets_stream");

        // Start streaming query
        StreamingQuery query = df
                .writeStream()
                .outputMode(OutputMode.Complete())
                .foreachBatch((batchDF, batchId) -> {
                    // Convert DataFrame to list of Documents
                    List<Document> documents = new ArrayList<>();
                    batchDF.collectAsList().forEach(row -> {
                        String country = row.getAs("hashtags");
                        long count = row.getAs("count");
                        Document document = new Document("hashtags", country)
                                .append("count", count)
                                .append("createdAt", new java.util.Date());
                        documents.add(document);
                    });

                    if (!documents.isEmpty()) {
                        // Save documents to MongoDB
                        collection.insertMany(documents);
                    }
                })
                .start();

        query.awaitTermination();
        mongoClient.close();


        hashtagCounts.writeStream().outputMode("complete").format("console").start();
        userTweets.writeStream().outputMode("complete").format("console").start();
        contentLikes.writeStream().outputMode("complete").format("console").start();  // change this line

        spark.streams().awaitAnyTermination();
    }

}
//spark-submit --class spark.streaming.SparkKafkaHashtagCount --master local  tweets-2-jar-with-dependencies.jar localhost:9092 tweets mySparkConsumerGroup >> tweetsCountv3
