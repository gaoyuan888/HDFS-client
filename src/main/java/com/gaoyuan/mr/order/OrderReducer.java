package com.gaoyuan.mr.order;

/**
 * 功能描述:
 *
 * @author yaoyizhou
 * @date 2020/1/31 19:48
 * @desc
 */

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {

            context.write(key, NullWritable.get());
        }
    }
}
