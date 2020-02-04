package com.gaoyuan.mr.oneindex;

/**
 * 功能描述:
 *
 * @author yaoyizhou
 * @date 2020/2/4 10:41
 * @desc
 */
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OneIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

        int sum = 0;

        // 1 累加求和
        for(IntWritable value: values){
            sum +=value.get();
        }

        v.set(sum);

        // 2 写出
        context.write(key, v);
    }
}
