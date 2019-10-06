package NaiveSearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;


public class WordEnumerator {
    public static IntWritable sum = new IntWritable(0);
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z0-9\\s+]"," "), " “–‑;\\'”(),.\t\n\"");

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().toLowerCase());
//                System.out.printf("The element is %s \n", word);
//                System.out.println(word);
                context.write(word, one);
            }
//            StringTokenizer itr = new StringTokenizer(value.toString());
//            String query = "the best best the query in the world";
//            String [] tokens = query.split(" ");
//            Map<String, Integer> map = new HashMap<String, Integer>();
//            for (int i = 0; i < tokens.length; i++) {
//                if (!map.containsKey(tokens[i]))
//                    map.put(tokens[i], 1);
//                else map.put(tokens[i], map.get(tokens[i])+1);
//                System.out.println(map);
//            }
//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken());
//                System.out.println(word);
//                context.write(word, one);
//            }
//        }

        }
    }
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += 0;
//            }
//            sum += 1;
//            result.set(sum);
//            sum =sum+1;
            sum.set(sum.get()+1);
//            result.set(sum.get()+1);
            context.write(key,sum);
        }
    }
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }

        Job job = Job.getInstance(conf, "relevance analizator");
        job.setJarByClass(RelevanceAnalizator.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


