package NaiveSearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.json.JSONObject;

import java.io.IOException;
import java.util.*;


public class DocumentCounter {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        private IntWritable id = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int file_id;
            JSONObject json = new JSONObject(value.toString().replaceAll("<[^>]*>", " "));

            file_id = json.getInt("id");

            StringTokenizer itr = new StringTokenizer(json.getString("text").toLowerCase());

            while (itr.hasMoreTokens()) {
                String str = itr.nextToken().toLowerCase().replaceAll("[^a-z\\-]", "");
                if (str.length() == 0) {
                    continue;
                }
                word.set(str);
                id.set(file_id);

                context.write(word, id);
            }
        }
    }


    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Set set = new HashSet();

            for (IntWritable val : values) {
                set.add(val);
            }

            result.set(set.size());

            context.write(key, result);
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        /*Check if output path (args[1])exist or not*/
        if(fs.exists(new Path(args[1]))){
            /*If exist delete the output path*/
            fs.delete(new Path(args[1]),true);
        }


        Job job = Job.getInstance(conf, "IDF counter");
        job.setJarByClass(DocumentCounter.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}

