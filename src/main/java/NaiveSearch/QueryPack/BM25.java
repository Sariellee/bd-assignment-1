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
public class BM25 {
    public static class MapJob extends Mapper<Object, Text, DoubleWritable, IntWritable> {


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString(); // read a line in file
                Configuration conf = context.getConfiguration();
                JSONObject json_query = new JSONObject(conf.get("query"));
                String[] tokens = line.split("\t"); // split it into words
                JSONObject json_doc = new JSONObject(tokens[1]); // doc id
                double relevance = 0;

                for (Iterator it = json_query.keys(); it.hasNext(); ) { //for each word in a query
                    Object k = it.next(); //for next word
                    String k1 = k.toString();
                    String[] tfidf = json_doc.getString(k1).split("="); //tfidf for a word for a doc
                    int tf = Integer.parseInt(tfidf[0]); //tf for a doc
                    double idf = (double) 1 / Integer.parseInt(tfidf[1]); //and its idf
                    idf *= idf;
                    relevance += idf * tf * json_query.getDouble(k1);
                }
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
                context.write(val, key);
            }
        }
    }
}
