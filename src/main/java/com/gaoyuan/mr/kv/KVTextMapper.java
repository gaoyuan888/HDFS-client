package com.gaoyuan.mr.kv;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 功能描述:
 *
 * @author yaoyizhou
 * @date 2020/1/29 20:33
 * @desc
 */
public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable> {

    // 1 设置value
    LongWritable v = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

       // banzhang ni hao

        // 2 写出
        context.write(key, v);
    }
}