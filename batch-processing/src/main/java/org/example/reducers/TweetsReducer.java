package org.example.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TweetsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable totalCost = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
            System.out.println("value: " + val.get());
        }
        totalCost.set(sum);
        System.out.println("--> Sum = " + sum);
        context.write(key, totalCost);
    }
}
