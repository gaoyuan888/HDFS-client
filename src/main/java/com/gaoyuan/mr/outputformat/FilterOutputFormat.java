package com.gaoyuan.mr.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 功能描述:
 *
 * @author yaoyizhou
 * @date 2020/2/1 12:06
 * @desc
 */
public class FilterOutputFormat extends FileOutputFormat<Text,NullWritable> {
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new FilterRecordWriter(job);
    }
}
