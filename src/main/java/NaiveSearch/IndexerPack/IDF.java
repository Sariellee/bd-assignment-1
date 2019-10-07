package NaiveSearch.IndexerPack;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
/**
 * IDF job, returns document as the key and frequency as value.
 */
public class IDF {
    public static class MapJob extends Mapper<Object, Text, Text, HelperStructures.DocCount> {

        private HelperStructures.DocCount dc = new HelperStructures.DocCount();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t");
            dc.set(new IntWritable(Integer.parseInt(tokens[2])), new IntWritable(Integer.parseInt(tokens[1])));
            context.write(new Text(tokens[0]), dc);
        }
    }

    public static class ReduceJob extends Reducer<Text, HelperStructures.DocCount, HelperStructures.TermDocs, IntWritable> {

        public void reduce(Text key, Iterable<HelperStructures.DocCount> values, Context context) throws IOException, InterruptedException {
            HelperStructures.TermDocs docs = new HelperStructures.TermDocs();
            int sum = 0;
            ArrayList<HelperStructures.DocCount> objs = new ArrayList<HelperStructures.DocCount>();
            for (HelperStructures.DocCount val : values) {
                objs.add(new HelperStructures.DocCount(new IntWritable(val.getCount().get()), new IntWritable(val.getDocId().get())));
                sum++;
            }
            docs.setTerm(new Text(key.toString() + "=" + sum));
            for (int i = 0; i < objs.size(); i++) {
                HelperStructures.DocCount obj = objs.get(i);
                docs.setDocId(obj.getDocId());
                context.write(docs, obj.getCount());
            }
        }
    }

}
