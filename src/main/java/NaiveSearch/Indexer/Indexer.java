package NaiveSearch.Indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Indexer {
    public static void main(String[] args) throws Exception {


        Configuration IndexerJobConf = new Configuration();
        Configuration IDFJobConf = new Configuration();
        Configuration IFIDFJobConf = new Configuration();
        FileSystem fs = FileSystem.get(IndexerJobConf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        Job IndexerJob = Job.getInstance(IndexerJobConf, "Count Words Job");
        FileInputFormat.addInputPath(IndexerJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(IndexerJob, new Path(args[1]));

        IndexerJob.setMapOutputKeyClass(TermDocs.class);
        IndexerJob.setMapOutputValueClass(IntWritable.class);
        IndexerJob.setOutputKeyClass(TermDocs.class);
        IndexerJob.setOutputValueClass(IntWritable.class);

        IndexerJob.setJarByClass(CountWords.class);
        IndexerJob.setMapperClass(CountWords.MapJob.class);
        IndexerJob.setReducerClass(CountWords.ReduceJob.class);
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

        IDFJob.setJarByClass(IDF.class);
        IDFJob.setMapperClass(IDF.MapJob.class);
        IDFJob.setReducerClass(IDF.ReduceJob.class);
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

        IFIDFJob.setJarByClass(IFIDF.class);
        IFIDFJob.setMapperClass(IFIDF.MapJob.class);
        IFIDFJob.setReducerClass(IFIDF.ReduceJob.class);
        IFIDFJob.waitForCompletion(true);
    }
}
