package NaiveSearch.Indexer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

class IFIDF {
    public static class MapJob extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t");
            String[] word_tokens = tokens[0].split("=");
            String tfidf = word_tokens[0]+"\t"+tokens[2]+"="+word_tokens[1];
            context.write(new IntWritable(Integer.parseInt(tokens[1])),new Text(tfidf));

        }
    }

    public static class ReduceJob extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                JSONObject json = new JSONObject();
                for (Text val : values) {
                    String[] tokens = val.toString().split("\t");

                    json.put(tokens[0], tokens[1]);
                }
                context.write(key, new Text(json.toString()));
            } catch (JSONException e){

            }
        }
    }
}