package NaiveSearch.IndexerPack;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Count occurences of a word in document. Returns TermDocs(word, doc) as a key and # of occurences as value.
 */
public class CountWords {
    public static class MapJob extends Mapper<Object, Text, HelperStructures.TermDocs, IntWritable> {

        private IntWritable doc_id = new IntWritable();
        private Text term = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JSONObject json = new JSONObject(value.toString().replaceAll("<[^>]*>", " "));
                StringTokenizer itr = new StringTokenizer(json.getString("text"));
                doc_id.set(json.getInt("id"));
                while (itr.hasMoreTokens()) {
                    term.set(itr.nextToken().toLowerCase().replaceAll("[^a-z\\-]", ""));
                    if (!term.toString().equals("") && !term.toString().equals("-")) {
                        context.write(new HelperStructures.TermDocs(doc_id, term), one);
                    }
                }
            } catch (JSONException e) {

            }
        }
    }

    public static class ReduceJob extends Reducer<HelperStructures.TermDocs, IntWritable, HelperStructures.TermDocs, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(HelperStructures.TermDocs key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

}
