package com.gaoyuan.mr.custom;

/**
 * 功能描述:
 *
 * @author yaoyizhou
 * @date 2020/1/30 8:31
 * @desc
 */
import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context)		throws IOException, InterruptedException {

        context.write(key, values.iterator().next());
    }
}
