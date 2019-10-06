package NaiveSearch.Query;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

class ContentExtractor {
    static class MapJob extends Mapper<Object, Text, DoubleWritable, Text> {
        ArrayList<String> objs = new ArrayList<String>();

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            Configuration conf = context.getConfiguration();
            String param = conf.get("relevance");
            String[] tokens = param.split("\n");
            objs.addAll(Arrays.asList(tokens));
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject json = new JSONObject(value.toString().replaceAll("<[^>]*>", " "));
            for (String str: objs) {
                String[] tokens = str.split("\t");
                if (json.get("id").equals(tokens[0])) {
                    context.write(new DoubleWritable(-Double.parseDouble(tokens[1])), new Text(json.getString("title")+" "+json.get("url")));
                }
            }
        }
    }
    static class MyKeyComparator extends WritableComparator {

        protected MyKeyComparator(Class<? extends WritableComparable> keyClass) {
            super(keyClass);
        }
    }
    public static class ReduceJob extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val: values){
                key.set(-key.get());
                context.write(key, val);
            }
        }
    }
}