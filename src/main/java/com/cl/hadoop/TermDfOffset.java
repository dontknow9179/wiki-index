package com.cl.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class TermDfOffset {

    public static class Map extends Mapper<LongWritable, Text, Text, LongLongWritable> {
        public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            StringTokenizer itr = new StringTokenizer(text);
            String word = "";
            if (itr.hasMoreTokens()) {
                word = itr.nextToken();
            }
            long length = value.getLength() + 1;
            context.write(new Text(word),new LongLongWritable(offset.get(),length));
        }
    }


    public static class Reduce extends Reducer<Text, LongLongWritable, Text, TriLongWritable> {
        final long MAX_L=((long)1<<30)*((long)1<<30);
        public void reduce(Text word, Iterable<LongLongWritable> pairs, Context context) throws IOException, InterruptedException {
            long start = MAX_L;
            long len = 0,count = 0;
            for(LongLongWritable pair:pairs){
                start=Math.min(start,pair.x);
                len += pair.y;
                count++;
            }
            context.write(word,new TriLongWritable(count,start,len));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(TermDfOffset.class);
        job.setMapperClass(TermDfOffset.Map.class);
        job.setReducerClass(TermDfOffset.Reduce.class);
        job.setMapOutputValueClass(LongLongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TriLongWritable.class);
        job.setMapOutputValueClass(LongLongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
