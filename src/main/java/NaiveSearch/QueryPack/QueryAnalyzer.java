package NaiveSearch.QueryPack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Query analyzer job. Computes frequency of words in the query and relevance score for each document.
 */
public class QueryAnalyzer {
    public static class MapJob extends Mapper<Object, Text, DoubleWritable, IntWritable> {


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                Configuration conf = context.getConfiguration();
                JSONObject json_query = new JSONObject(conf.get("query"));
                String[] tokens = line.split("\t");
                JSONObject json_doc = new JSONObject(tokens[1]);
                double relevance = 0;
                double total = 0;

                for (Iterator it = json_query.keys(); it.hasNext(); ) {
                    Object k = it.next();
                    String k1 = k.toString();
                    String[] tfidf = json_doc.getString(k1).split("=");
                    int tf = Integer.parseInt(tfidf[0]);
                    double idf = (double) 1 / Integer.parseInt(tfidf[1]);
//                    idf *= idf;
                    total +=json_query.getDouble(k1);
                    relevance += idf * tf * json_query.getDouble(k1);
                }
                relevance = relevance /total;
                context.write(new DoubleWritable(relevance), new IntWritable(Integer.parseInt(tokens[0])));
            } catch (JSONException e) {

            }
        }
    }

    public static class ReduceJob extends Reducer<DoubleWritable, IntWritable, IntWritable, DoubleWritable> {
        private LinkedList<IntWritable> list = new LinkedList<IntWritable>();
        private int doc_count = 0;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            Configuration conf = context.getConfiguration();
            doc_count = conf.getInt("max", 10);
        }

        public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                if (list.size() >= doc_count || key.get() == 0.0) {
                    continue;
                }
                list.add(val);
                System.out.println(val);
                context.write(val, key);
            }
        }
    }
}
