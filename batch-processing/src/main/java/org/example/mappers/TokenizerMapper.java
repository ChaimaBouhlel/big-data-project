package org.example.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();
            int startIndex = line.indexOf("['");
            int endIndex = line.lastIndexOf("']");
            if (startIndex != -1 && endIndex != -1 && endIndex > startIndex) {
                String hashtags = line.substring(startIndex + 2, endIndex);
                String[] hashtagsArray = hashtags.split("',\\s*'");

                // Process hashtagsArray as needed
                for (String hashtag : hashtagsArray) {
                    word.set(hashtag);
                    System.out.println("Hashtag: " + hashtag);
                    context.write(word, one); // Emit each hashtag with count 1
                }
            } else {
                System.err.println("Invalid CSV format: " + value.toString());
            }
        } catch (Exception e) {
            // Log or handle any exceptions
            System.err.println("Error processing record: " + e.getMessage());
            // Optionally, you can write this record to a separate output for further analysis or processing
            context.getCounter("Custom Counters", "Other Errors").increment(1);
        }
    }

    private String[] parseHashtags(String hashtags) {
        System.out.println("Hashtags before: " + hashtags);
        if (hashtags == null || hashtags.equalsIgnoreCase("None")) {
            return new String[0]; // Return an empty array if hashtags are null or "None"
        } else if (hashtags.startsWith("['") && hashtags.endsWith("']")) {
            // Assuming hashtags are in the format "['tag1', 'tag2', ...]"
            hashtags = hashtags.substring(1, hashtags.length() - 1); // Remove square brackets
            System.out.println("Hashtags: " + hashtags);
            return hashtags.split(",\\s*"); // Split by comma and optional whitespace
        } else {
            // Handle other cases if needed
            return new String[] { hashtags.trim() }; // Return a single-element array with the hashtag
        }
    }
}
