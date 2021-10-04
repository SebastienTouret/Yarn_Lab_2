package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpeciesReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key,Iterable<IntWritable> values,Context context)
            throws IOException,InterruptedException{
        int sum=0;
        for(IntWritable val:values){
            // Ignoring null values
            if (val == null)
                continue;
            sum+= val.get();
        }
        result.set(sum);
        context.write(key,result);
    }
}