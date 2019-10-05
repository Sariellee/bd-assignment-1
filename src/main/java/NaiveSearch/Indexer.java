package NaiveSearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class Indexer {
    public static class MapJob extends Mapper<Object, Text, IntWritable, MyMapWritable> {

        private IntWritable doc_id = new IntWritable();
        private Text term = new Text();
        private final static IntWritable one = new IntWritable(1);
        private static MyMapWritable word_count = new MyMapWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject json = new JSONObject(value.toString().replaceAll("<[^>]*>", " "));
            StringTokenizer itr = new StringTokenizer(json.getString("text"));
            doc_id.set(json.getInt("id"));
            while (itr.hasMoreTokens()) {
                term.set(itr.nextToken().toLowerCase().replaceAll("[^a-z\\-]", ""));
                word_count.put(term, one);
                context.write(doc_id, word_count);
            }
        }
    }

    public static class ReduceJob extends Reducer<IntWritable, MyMapWritable, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<MyMapWritable> values, Context context) throws IOException, InterruptedException {
            JSONObject json = new JSONObject();
            for (MyMapWritable val : values) {
                for (Writable inner_val : val.keySet()){
                    if (json.has(inner_val.toString())){
                        json.put(inner_val.toString(), json.getInt(inner_val.toString())+val.get(inner_val).get());
                    } else{
                        json.put(inner_val.toString(), val.get(inner_val).get());
                    }

                }
            }
            System.out.println(json.toString());
            context.write(key, new Text(json.toString()));
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

        indexerJob.setMapOutputKeyClass(IntWritable.class);
        indexerJob.setMapOutputValueClass(MyMapWritable.class);
        indexerJob.setOutputKeyClass(IntWritable.class);
        indexerJob.setOutputValueClass(Text.class);

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
    
    static class MyMapWritable extends AbstractMapWritable implements Map<Writable, IntWritable> {
        private Map<Writable, IntWritable> instance;

        MyMapWritable() {
            this.instance = new HashMap();
        }

        public MyMapWritable(MyMapWritable other) {
            this();
            this.copy(other);
        }

        public void clear() {
            this.instance.clear();
        }

        public boolean containsKey(Object key) {
            return this.instance.containsKey(key);
        }

        public boolean containsValue(Object value) {
            return this.instance.containsValue(value);
        }

        public Set<Entry<Writable, IntWritable>> entrySet() {
            return this.instance.entrySet();
        }

        public IntWritable get(Object key) {
            return (IntWritable)this.instance.get(key);
        }

        public boolean isEmpty() {
            return this.instance.isEmpty();
        }

        public Set<Writable> keySet() {
            return this.instance.keySet();
        }

        public IntWritable put(Writable key, IntWritable value) {
            this.addToMap(key.getClass());
            this.addToMap(value.getClass());
            return (IntWritable)this.instance.put(key, value);
        }

        public void putAll(Map<? extends Writable, ? extends IntWritable> t) {
            Iterator i$ = t.entrySet().iterator();

            while(i$.hasNext()) {
                Entry<? extends Writable, ? extends Writable> e = (Entry)i$.next();
                this.put((Writable)e.getKey(), (IntWritable)e.getValue());
            }

        }

        public IntWritable remove(Object key) {
            return (IntWritable)this.instance.remove(key);
        }

        public int size() {
            return this.instance.size();
        }

        public Collection<IntWritable> values() {
            return this.instance.values();
        }

        public void write(DataOutput out) throws IOException {
            super.write(out);
            out.writeInt(this.instance.size());
            Iterator i$ = this.instance.entrySet().iterator();

            while(i$.hasNext()) {
                Entry<Writable, IntWritable> e = (Entry)i$.next();
                out.writeByte(this.getId(((Writable)e.getKey()).getClass()));
                e.getKey().write(out);
                out.writeByte(this.getId(((IntWritable)e.getValue()).getClass()));
                e.getValue().write(out);
            }

        }
        public void readFields(DataInput in) throws IOException {
            super.readFields(in);
            this.instance.clear();
            int entries = in.readInt();

            for(int i = 0; i < entries; ++i) {
                Writable key = (Writable) ReflectionUtils.newInstance(this.getClass(in.readByte()), this.getConf());
                key.readFields(in);
                IntWritable value = (IntWritable)ReflectionUtils.newInstance(this.getClass(in.readByte()), this.getConf());
                value.readFields(in);
                this.instance.put(key, value);
            }
        }
    }
}
