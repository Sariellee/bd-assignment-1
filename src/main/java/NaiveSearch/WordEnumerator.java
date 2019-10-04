package NaiveSearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordEnumerator {

    public enum CountersEnum {INPUT_WORDS}

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject json = new JSONObject(value.toString().replaceAll("<[^>]*>", " "));
            StringTokenizer itr = new StringTokenizer(json.getString("text"));

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().toLowerCase().replaceAll("[^a-z\\-]", ""));
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.INPUT_WORDS.toString());
            counter.increment(1);
            result.set((int) counter.getValue());
            context.write(key, result);


        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // removing input folder if it exists
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordEnumerator.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
