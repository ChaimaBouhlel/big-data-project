package org.example.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class ScoreMapper extends Mapper<Object, Text, IntWritable, Text> {
    private final IntWritable score = new IntWritable();
    private final Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\s+");
        if (parts.length == 2) {
            word.set(parts[0]);
            score.set(Integer.parseInt(parts[1]));
            context.write(score, word);
        }
    }
}