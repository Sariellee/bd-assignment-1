package NaiveSearch.Indexer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

class IDFJob {
    public static class MapJob extends Mapper<Object, Text, Text, DocCount> {

        private DocCount dc = new DocCount();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t");
            dc.set(new IntWritable(Integer.parseInt(tokens[2])), new IntWritable(Integer.parseInt(tokens[1])));
            context.write(new Text(tokens[0]), dc);
        }
    }

    public static class ReduceJob extends Reducer<Text, DocCount, TermDocs, IntWritable> {

        public void reduce(Text key, Iterable<DocCount> values, Context context) throws IOException, InterruptedException {
            TermDocs docs = new TermDocs();
            int sum = 0;
            ArrayList<DocCount> objs = new ArrayList<DocCount>();
            for (DocCount val : values) {
                objs.add(new DocCount(new IntWritable(val.getCount().get()), new IntWritable(val.getDocId().get())));
                sum++;
            }
            docs.setTerm(new Text(key.toString() + "=" + sum));
            for (int i = 0; i < objs.size(); i++) {
                DocCount obj = objs.get(i);
                docs.setDocId(obj.getDocId());
                context.write(docs, obj.getCount());
            }
        }
    }

}


class DocCount implements Writable {
    private IntWritable docId;
    private IntWritable count;

    DocCount() {
        this.docId = new IntWritable();
        this.count = new IntWritable();
    }

    DocCount(IntWritable count, IntWritable term) {
        this.docId = term;
        this.count = count;
    }

    public IntWritable getDocId() {
        return docId;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    public void setDocId(IntWritable docId) {
        this.docId = docId;
    }

    public void set(IntWritable count, IntWritable docId) {
        this.count = count;
        this.docId = docId;
    }


    public void readFields(DataInput in) throws IOException {
        docId.readFields(in);
        count.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        docId.write(out);
        count.write(out);
    }

    @Override
    public String toString() {
        return docId.toString() + "\t" + count.toString();
    }
}

