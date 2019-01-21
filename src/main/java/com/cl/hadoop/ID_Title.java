package com.cl.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wikiclean.WikiClean;

import java.io.IOException;

public class ID_Title {
    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
        //输入：key为该页面在xml中的起始位置offset，value为该页面的xml内容
        //输出：key为文章id，value为文章标题
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            WikiClean cleaner = new WikiClean.Builder().withTitle(true).build();
            String text_id = cleaner.getId(text);
            LongWritable id = new LongWritable(Long.parseLong(text_id));
            String text_title = cleaner.getTitle(text);
            Text title = new Text(text_title);
            context.write(id,title);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(conf);
        job.setJarByClass(ID_Title.class);
        job.setMapperClass(ID_Title.Map.class);
        job.setInputFormatClass(XMLInputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
