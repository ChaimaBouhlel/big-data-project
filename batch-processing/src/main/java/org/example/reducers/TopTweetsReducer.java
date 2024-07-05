package org.example.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class TopTweetsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private TreeMap<Integer, Text> topUsers = new TreeMap<>();
    private Map<Text, Integer> userTotalCounts = new HashMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Add the user and their total count to the TreeMap
        topUsers.put(sum, new Text(key.toString()));

        // Keep only the top 10 users in the TreeMap
        if (topUsers.size() > 10) {
            topUsers.remove(topUsers.firstKey());
        }

        // Store the total count per user in the userTotalCounts map
        userTotalCounts.put(new Text(key.toString()), sum);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output the top 10 users and their total count
        for (Text user : topUsers.descendingMap().values()) {
            context.write(user, new IntWritable(topUsers.lastKey()));
        }

        // Output the total count per user
        for (Map.Entry<Text, Integer> entry : userTotalCounts.entrySet()) {
            context.write(entry.getKey(), new IntWritable(entry.getValue()));
        }
    }
}
