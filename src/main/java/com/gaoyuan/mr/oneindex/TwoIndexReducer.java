package com.gaoyuan.mr.oneindex;

/**
 * 功能描述:
 *
 * @author yaoyizhou
 * @date 2020/2/4 12:31
 * @desc
 */
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {

    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // atguigu a.txt 3
        // atguigu b.txt 2
        // atguigu c.txt 2

        // atguigu c.txt-->2 b.txt-->2 a.txt-->3

        StringBuilder sb = new StringBuilder();

        // 1 拼接
        for (Text value : values) {
            sb.append(value.toString().replace("\t", "-->") + "\t");
        }

        v.set(sb.toString());

        // 2 写出
        context.write(key, v);
    }
}
