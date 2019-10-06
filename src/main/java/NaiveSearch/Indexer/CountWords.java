package NaiveSearch.Indexer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;

import java.io.IOException;
import java.util.StringTokenizer;

public class CountWords {
    public static class MapJob extends Mapper<Object, Text, TermDocs, IntWritable> {

        private IntWritable doc_id = new IntWritable();
        private Text term = new Text();
        private final static IntWritable one = new IntWritable(1);


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject json = new JSONObject(value.toString().replaceAll("<[^>]*>", " "));
            StringTokenizer itr = new StringTokenizer(json.getString("text"));
            doc_id.set(json.getInt("id"));
            while (itr.hasMoreTokens()) {
                term.set(itr.nextToken().toLowerCase().replaceAll("[^a-z\\-]", ""));
                context.write(new TermDocs(doc_id, term), one);
            }
        }
    }

    public static class ReduceJob extends Reducer<TermDocs, IntWritable, TermDocs, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(TermDocs key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

}
