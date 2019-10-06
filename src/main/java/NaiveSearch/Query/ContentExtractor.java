package NaiveSearch.Query;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

class ContentExtractor {
    public enum CountersEnum {RANK}

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
                    context.write(new DoubleWritable(Double.parseDouble(tokens[1])), new Text(json.getString("title")+" "+json.get("url")));
                }
            }
        }
    }
    public static class ReduceJob extends Reducer<DoubleWritable, Text, IntWritable, Text> {
        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
        }
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.RANK.toString());
            for (Text val: values){
                counter.increment(1);
                context.write(new IntWritable((int) counter.getValue()), val);
            }
        }
    }
    static class ReverseDoubleComparator extends WritableComparator {
        private static final DoubleWritable.Comparator comparator = new DoubleWritable.Comparator();
        public ReverseDoubleComparator() {
            super(DoubleWritable.class);
        }
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return (-1)*comparator.compare(b1, s1, l1, b2, s2, l2);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return (-1)*super.compare(a, b);
        }
    }
}