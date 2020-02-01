package com.gaoyuan.mr.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 功能描述:
 *
 * @author yaoyizhou
 * @date 2020/2/1 12:02
 * @desc
 */
public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    Text text = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {



        String s = key.toString();
         s = s + "\r\n";

        text.set(s);

        //防止有重复的
        for (NullWritable value : values) {
            context.write(text, NullWritable.get());
        }

    }
}
