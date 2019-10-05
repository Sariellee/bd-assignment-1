package NaiveSearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import javax.print.Doc;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
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
                sum+= val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration firstJobConf = new Configuration();
        Configuration secondJobConf = new Configuration();
        FileSystem fs = FileSystem.get(firstJobConf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }

        Job firstJob = Job.getInstance(firstJobConf, "Indexer Job");
        FileInputFormat.addInputPath(firstJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(firstJob, new Path(args[1]));

        firstJob.setMapOutputKeyClass(TermDocs.class);
        firstJob.setMapOutputValueClass(IntWritable.class);
        firstJob.setOutputKeyClass(TermDocs.class);
        firstJob.setOutputValueClass(IntWritable.class);

        firstJob.setJarByClass(Indexer.class);
        firstJob.setMapperClass(Indexer.MapJob.class);
        firstJob.setReducerClass(Indexer.ReduceJob.class);
        firstJob.waitForCompletion(true);

        Job secondJob = Job.getInstance(secondJobConf, "Second Job");
        FileInputFormat.addInputPath(secondJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(secondJob, new Path(args[2]));
        if(fs.exists(new Path(args[2]))){
            fs.delete(new Path(args[2]),true);
        }

        secondJob.setMapOutputKeyClass(Text.class);
        secondJob.setMapOutputValueClass(DocCount.class);
        secondJob.setOutputKeyClass(TermDocs.class);
        secondJob.setOutputValueClass(IntWritable.class);

        secondJob.setJarByClass(SecondJob.class);
        secondJob.setMapperClass(SecondJob.MapJob.class);
        secondJob.setReducerClass(SecondJob.ReduceJob.class);
        secondJob.waitForCompletion(true);
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
            return term.toString() + "\t" + docId.toString();
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

    static class SecondJob {
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
            private IntWritable result = new IntWritable();
            private TermDocs docs = new TermDocs();

            public void reduce(Text key, Iterable<DocCount> values, Context context) throws IOException, InterruptedException {
                int sum = 0;
                ArrayList<DocCount> objs = new ArrayList<DocCount>();
                for (DocCount val: values){
                    objs.add(val);
                    sum++;
                }
                docs.setTerm(new Text(key.toString() + "="+sum));
                for(int i = 0; i < objs.size(); i++)
                {
                    DocCount obj = objs.get(i);
                    docs.setDocId(obj.docId);
                    context.write(docs, obj.count);
                }
            }
        }

    }

    static class WordCount implements Writable {
        private Text term;
        private IntWritable count;

        WordCount() {
            this.term = new Text();
            this.count = new IntWritable();
        }

        WordCount(IntWritable count, Text term) {
            this.term = term;
            this.count = count;
        }

        public Text getTerm() {
            return term;
        }

        public IntWritable getCount() {
            return count;
        }

        public void setCount(IntWritable count) {
            this.count = count;
        }

        public void setTerm(Text term) {
            this.term = term;
        }

        public void set(IntWritable count, Text term){
            this.count = count;
            this.term = term;
        }


        public void readFields(DataInput in) throws IOException {
            term.readFields(in);
            count.readFields(in);
        }

        public void write(DataOutput out) throws IOException {
            term.write(out);
            count.write(out);
        }

        @Override
        public String toString() {
            return term.toString() + "\t" + count.toString();
        }
    }

    static class DocCount implements Writable {
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

        public void set(IntWritable count, IntWritable docId){
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

}
