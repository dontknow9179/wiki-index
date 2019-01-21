package com.cl.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wikiclean.WikiClean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TermPosition {
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

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            WikiClean cleaner = new WikiClean.Builder().withTitle(true).build();
            String content = cleaner.clean(text).toLowerCase();
            if(content.indexOf("#redirect")!=-1) return;

            HashMap<String, List<Long>> count = new HashMap<String, List<Long>>();
            //Long id = Long.parseLong(cleaner.getId(text));
            String id_s = cleaner.getId(text);
            String[] words = split(content.toCharArray());
            List<Long> longList = new ArrayList<>();
            int n = words.length;
            int start = 0;
            int index = 0;
            for (int i = 0; i < n; i++) {
                String word = words[i];
                if (!count.containsKey(word)) {
                    List<Long> longList_ = new ArrayList<>();
                    index = content.indexOf(word,index);
                    longList_.add(new Long((long)index));
                    count.put(word, longList_);
                    index += word.length();
                }
                else{
                    index = content.indexOf(word,index);
                    longList = count.get(word);
                    longList.add(new Long((long)index));
                    count.put(word, longList);
                    index += word.length();
                }

            }
            String term_id = "";
            for (HashMap.Entry<String, List<Long>> entry : count.entrySet()) {
                String key = entry.getKey();
                longList = entry.getValue();

                String pl = "( ";
                for(int i = 0; i < longList.size(); i++){
                    pl = pl + String.valueOf(longList.get(i)) + " ";
                }
                pl = pl + ")";
                term_id = key + "," + id_s;
                context.write(new Text(term_id), new Text(pl));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(conf);
        job.setJarByClass(TermPosition.class);
        job.setMapperClass(TermPosition.Map.class);
        job.setInputFormatClass(XMLInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
