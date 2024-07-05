package org.example.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class TopTweetsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text username = new Text();
    private IntWritable totalValue = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] fields = value.toString().split(",", -1);
            System.out.println("Fields: " + Arrays.toString(fields));
            if (fields.length >= 12) { // Ensure all required fields are present
                String userNameValue = fields[9].trim();
                int likeCount = Integer.parseInt(fields[3].trim());
                int quoteCount = Integer.parseInt(fields[4].trim());
                int replyCount = Integer.parseInt(fields[5].trim());
                int retweetCount = Integer.parseInt(fields[6].trim());
                int total = likeCount + quoteCount + replyCount + retweetCount;
                username.set(userNameValue);
                totalValue.set(total);
                System.out.println(username + " " + totalValue);
                context.write(username, totalValue);
            } else {
                System.err.println("Invalid CSV format: " + value.toString());
            }
        } catch (NumberFormatException e) {
            // Log or handle the parsing error
            System.err.println("Error parsing numeric value in CSV: " + e.getMessage());
            // Optionally, you can write this record to a separate output for further analysis or processing
            context.getCounter("Custom Counters", "Parsing Errors").increment(1);
        } catch (Exception e) {
            // Log or handle any other exceptions
            System.err.println("Error processing record: " + e.getMessage());
            // Optionally, you can write this record to a separate output for further analysis or processing
            context.getCounter("Custom Counters", "Other Errors").increment(1);
        }
    }
}
