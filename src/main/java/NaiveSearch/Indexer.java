package NaiveSearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

public class Indexer {
    public static class MapJob extends Mapper<Object, Text, TermDocs, IntWritable > {

        private IntWritable doc_id = new IntWritable();
        private Text term = new Text();
        private final static IntWritable one = new IntWritable(1);


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject json = new JSONObject(value.toString().replaceAll("<[^>]*>", " "));
            StringTokenizer itr = new StringTokenizer(json.getString("text"));
            doc_id.set(json.getInt("id"));
            while (itr.hasMoreTokens()) {
                System.out.println(term);
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
                System.out.println(val.toString());
                sum+= val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration indexerJobConf = new Configuration();
        FileSystem fs = FileSystem.get(indexerJobConf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }

        Job indexerJob = Job.getInstance(indexerJobConf, "Indexer Job");
        FileInputFormat.addInputPath(indexerJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(indexerJob, new Path(args[1]));

        indexerJob.setMapOutputKeyClass(TermDocs.class);
        indexerJob.setMapOutputValueClass(IntWritable.class);
        indexerJob.setOutputKeyClass(TermDocs.class);
        indexerJob.setOutputValueClass(IntWritable.class);

        indexerJob.setJarByClass(Indexer.class);
        indexerJob.setMapperClass(Indexer.MapJob.class);
        indexerJob.setReducerClass(Indexer.ReduceJob.class);
        indexerJob.waitForCompletion(true);
    }
    static class TermDocs implements WritableComparable<TermDocs> {
        private Text term;
        private IntWritable docId;

        TermDocs(){
            this.term = new Text();
            this.docId = new IntWritable();
        }

        TermDocs(IntWritable docId, Text term) {
            this.term = term;
            this.docId = docId;
        }

        public Text getTerm() {
            return term;
        }

        public IntWritable getDocId() {
            return docId;
        }

        public void setDocId(IntWritable docId) {
            this.docId = docId;
        }

        public void setTerm(Text term) {
            this.term = term;
        }


        public void readFields(DataInput in) throws IOException {
            term.readFields(in);
            docId.readFields(in);
        }

        public void write(DataOutput out) throws IOException {
            term.write(out);
            docId.write(out);
        }

        @Override
        public String toString() {
            return "("+term.toString() + "\t" + docId.toString()+")";
        }

        public int compareTo(TermDocs termDocs) {
            if (this.docId.compareTo(termDocs.docId) == 0){
                return this.term.compareTo(termDocs.term);
            }
            else{
                return this.docId.compareTo(termDocs.docId);
            }
        }
    }

}
