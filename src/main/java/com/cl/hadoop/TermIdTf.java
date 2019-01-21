package com.cl.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wikiclean.WikiClean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TermIdTf {

    public static String[] split(char[] text){
        String pattern = "[a-zA-Z]+('[a-zA-Z]+)?(_[a-zA-Z]+)?";
        ArrayList<String> words = new ArrayList<String>();
        String t = new String(text);
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(t);
        while(m.find()) {
            words.add(m.group());
        }
        return words.toArray(new String[]{});
    }
    public static boolean clean(String word){
        if(word.equals("a")||word.equals("an")||word.equals("the")||word.equals("it")||word.equals("to")||
                word.equals("is")||word.equals("are"))
            return true;
        return false;
    }

    public static class Map extends Mapper<LongWritable, Text, StringDoubleWritable, LongWritable> {
        //private final static IntWritable one = new IntWritable(1);

        //输入：key为该页面在xml中的起始位置offset，value为该页面的xml内容
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            WikiClean cleaner = new WikiClean.Builder().withTitle(true).build();

            HashMap<String, Long> count = new HashMap<String, Long>();

            String content = cleaner.clean(text).toLowerCase();
            if(content.indexOf("#redirect")!=-1) return;

            //String id_s = cleaner.getId(text);
            LongWritable id = new LongWritable(Long.parseLong(cleaner.getId(text)));
            //Long id = Long.parseLong(cleaner.getId(text));
            String title = cleaner.getTitle(text).toLowerCase();
            Long length = new Long((long)content.length());

            String[] words = split(content.toCharArray());
            int n = words.length;
            for (int i = 0; i < n; i++) {
                String word = words[i];
                if(clean(word)) continue;
                if (!count.containsKey(word)) count.put(word, new Long(0));
                count.put(word, count.get(word) + 1);
            }

            for (HashMap.Entry<String, Long> entry : count.entrySet()) {
                String key_ = entry.getKey();
                double tf = 1.0 * entry.getValue() / length;
                int inTitle = (title.indexOf(key_) == -1) ? 0 : 1;
                double tf_ = (0.85 * tf + 0.15 * inTitle) * 1000.0;
                context.write(new StringDoubleWritable(key_, tf_),id);

            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(conf);
        job.setJarByClass(TermIdTf.class);
        job.setMapperClass(TermIdTf.Map.class);
        job.setInputFormatClass(XMLInputFormat.class);
        job.setOutputKeyClass(StringDoubleWritable.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
