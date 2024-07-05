package org.example.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TweetsCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text username = new Text();
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // Split using regex to handle quoted fields
        if (fields.length >= 13) { // Ensure all required fields are present
            String userNameValue = fields[9].trim();
            // Remove quotes from the username field if present
            userNameValue = userNameValue.replaceAll("^\"|\"$", "");
            username.set(userNameValue);
            context.write(username, one);
        } else {
            //System.err.println("Invalid CSV format: " + value.toString());
        }
    }
}
