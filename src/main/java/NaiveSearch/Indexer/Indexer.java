package NaiveSearch.Indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.IOException;
import java.util.StringTokenizer;

public class Indexer {
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

    public static void main(String[] args) throws Exception {
        Configuration IndexerJobConf = new Configuration();
        Configuration IDFJobConf = new Configuration();
        Configuration IFIDFJobConf = new Configuration();
        FileSystem fs = FileSystem.get(IndexerJobConf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        Job IndexerJob = Job.getInstance(IndexerJobConf, "Indexer Job");
        FileInputFormat.addInputPath(IndexerJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(IndexerJob, new Path(args[1]));

        IndexerJob.setMapOutputKeyClass(TermDocs.class);
        IndexerJob.setMapOutputValueClass(IntWritable.class);
        IndexerJob.setOutputKeyClass(TermDocs.class);
        IndexerJob.setOutputValueClass(IntWritable.class);

        IndexerJob.setJarByClass(Indexer.class);
        IndexerJob.setMapperClass(Indexer.MapJob.class);
        IndexerJob.setReducerClass(Indexer.ReduceJob.class);
        IndexerJob.waitForCompletion(true);

        Job IDFJob = Job.getInstance(IDFJobConf, "IDF Job");
        FileInputFormat.addInputPath(IDFJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(IDFJob, new Path(args[2]));
        if (fs.exists(new Path(args[2]))) {
            fs.delete(new Path(args[2]), true);
        }

        IDFJob.setMapOutputKeyClass(Text.class);
        IDFJob.setMapOutputValueClass(DocCount.class);
        IDFJob.setOutputKeyClass(TermDocs.class);
        IDFJob.setOutputValueClass(IntWritable.class);

        IDFJob.setJarByClass(IDFJob.class);
        IDFJob.setMapperClass(IDFJob.MapJob.class);
        IDFJob.setReducerClass(IDFJob.ReduceJob.class);
        IDFJob.waitForCompletion(true);


        Job IFIDFJob = Job.getInstance(IFIDFJobConf, "IFIDF Job");
        FileInputFormat.addInputPath(IFIDFJob, new Path(args[2]));
        FileOutputFormat.setOutputPath(IFIDFJob, new Path(args[3]));
        if (fs.exists(new Path(args[3]))) {
            fs.delete(new Path(args[3]), true);
        }

        IFIDFJob.setMapOutputKeyClass(IntWritable.class);
        IFIDFJob.setMapOutputValueClass(WordIFIDF.class);
        IFIDFJob.setOutputKeyClass(IntWritable.class);
        IFIDFJob.setOutputValueClass(Text.class);

        IFIDFJob.setJarByClass(IFIDFJob.class);
        IFIDFJob.setMapperClass(IFIDFJob.MapJob.class);
        IFIDFJob.setReducerClass(IFIDFJob.ReduceJob.class);
        IFIDFJob.waitForCompletion(true);
    }


}
