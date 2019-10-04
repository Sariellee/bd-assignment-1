package NaiveSearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;


public class DocumentCounter {
    static Integer ID = 0;
    static Map<Integer, String> dictionary_id_file = new HashMap<Integer, String>();
    static Map<String, Integer> dictionary_file_id = new HashMap<String, Integer>();

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        private IntWritable id = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Integer file_id;
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String file_name = fileSplit.getPath().getName();

            if (dictionary_file_id.containsKey(file_name)){
                file_id = dictionary_file_id.get(file_name);
            }else {
                file_id = ID;
                ID = ID + 1;
                dictionary_file_id.put(file_name, file_id);
                dictionary_id_file.put(file_id, file_name);
            }


            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase());

            while (itr.hasMoreTokens()) {
                String str = itr.nextToken().replaceAll("[^a-zA-Z]", "");
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
            int sum = 0;
            for (IntWritable val : values) {
                sum += 1;
            }

            result.set(sum);

            context.write(key, result);
        }
    }



    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IDF counter");
        job.setJarByClass(DocumentCounter.class);
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

