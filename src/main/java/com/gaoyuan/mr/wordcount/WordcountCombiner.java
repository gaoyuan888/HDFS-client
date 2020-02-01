package com.gaoyuan.mr.wordcount;

/**
 * 功能描述:
 *
 * @author yaoyizhou
 * @date 2020/1/31 18:23
 * @desc
 */
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordcountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // 1 汇总
        int sum = 0;

        for(IntWritable value :values){
            sum += value.get();
        }

        v.set(sum);

        // 2 写出
        context.write(key, v);
    }
}
