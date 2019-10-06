package NaiveSearch;


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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;


public class QueryVectorizer {
    public static Map<String, Integer> map = new HashMap<String, Integer>();
    public static Map<String, Integer> map_total = new HashMap<String, Integer>();
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text id = new Text();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z0-9\\s+]"," "), " “–‑;\\'”(),.\t\n\"");
            StringTokenizer itr = new StringTokenizer(value.toString());
//            String [] doc = value.toString().split(" ");
//            System.out.println(map);
            String ind = itr.nextToken();
            JSONObject dict = new JSONObject(itr.nextToken());
            for (String el:map.keySet()){
                System.out.printf("el: %s, value %s", el, dict.get(el));
                if (dict.has(el)) {
                    if (!map_total.containsKey(el))
                        map_total.put(el, 1);
                    else map_total.put(el, map_total.get(el)+1);

//                System.out.printf("From JSON %s %f", el, dict.getFloat(el));
                    id.set(el);
                    context.write(new Text(ind), new Text(el + "_"+dict.get(el)));
                }
            }
//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken().toLowerCase());
////                System.out.printf("The element is %s \n", word);
////                System.out.println(word);
//                context.write(word, one);
//            }

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
    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            for (Text val : values) {
                String s = "";
                 StringTokenizer itr = new StringTokenizer(val.toString().trim(), "_");
                 s= itr.nextToken();
                System.out.printf("First: %s", s);


                try{

                    String d = itr.nextToken();
                    System.out.printf(", second: %s\n", d);

                }catch(NoSuchElementException t){
                    System.out.printf(", second: %s\n", "NONE");

                }


            }
            context.write(key, new Text("0"));

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
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



