package NaiveSearch;

import netscape.javascript.JSObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;


public class RelevanceAnalizator {
    public static Map<String, Integer> map = new HashMap<String, Integer>();
    public static Map<String, Integer> map_total = new HashMap<String, Integer>();
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {


        private final static IntWritable one = new IntWritable(1);
        private Text term = new Text();
        private Text id = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
//            String [] doc = value.toString().split(" ");
//            System.out.println(map);
            String ind = itr.nextToken();
            JSONObject dict = new JSONObject(itr.nextToken());
            for (String el:map.keySet()){
                if (dict.has(el)) {
                    if (!map_total.containsKey(el))
                        map_total.put(el, 1);
                    else map_total.put(el, map_total.get(el)+1);

//                System.out.printf("From JSON %s %f", el, dict.getFloat(el));
                    id.set(el);
                    context.write(id, new Text(dict.getFloat(el) + " " + ind));
                }
            }


//            System.out.printf("The index is %s \n",itr.nextToken());
//            System.out.printf("The array is %s \n",itr.nextToken());



//                term.set("1");
////                System.out.println(word);
//                context.write(term, one);

        }

    }
    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
            int sum = 0;
//            System.out.println(map_total);

//            String [] doc = values.toString().split(" ", 10);
            for (String val : map_total.keySet()) {
//                sum = 0;
//                StringTokenizer itr = new StringTokenizer(val.toString());
//                System.out.printf("First el %s, second el %s", itr.nextToken(), itr.nextToken());
//                String ind = itr.nextToken();
//                String doc = itr.nextToken();
//                String doc = itr.nextToken();
//                sum += Integer.parseInt(itr.nextToken());
//                sum+=1;
//                result.set(Integer.toString(sum));


//            System.out.println(map);
                result.set("0");
                context.write(key,result);

            }

//
//
//
//            }
//            result.set(Integer.toString(sum));
//            context.write(key,result);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }
        String query = "the best best the query in the world";
        String [] tokens = query.split(" ");

        for (int i = 0; i < tokens.length; i++) {
            if (!map.containsKey(tokens[i]))
                map.put(tokens[i], 1);
            else map.put(tokens[i], map.get(tokens[i])+1);

        }
        Job job = Job.getInstance(conf, "relevance analizator");
        job.setJarByClass(RelevanceAnalizator.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

