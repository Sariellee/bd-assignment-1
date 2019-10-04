package NaiveSearch;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class Indexer {
    public static void main(String[] args) throws Exception {
        Configuration enumeratorConf = new Configuration();
        Configuration docCountConf = new Configuration();
        Configuration indexerJobConf = new Configuration();

        Job enumJob = Job.getInstance(enumeratorConf, "Enumerator Job");
        FileInputFormat.addInputPath(enumJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(enumJob, new Path(args[1]));

        enumJob.setJarByClass(Enumerator.class);
        enumJob.setMapperClass(Enumerator.Map.class);
        enumJob.setReducerClass(Enumerator.Reduce.class);

        enumJob.setMapOutputKeyClass(Text.class);
        enumJob.setMapOutputValueClass(IntWritable.class);
        enumJob.setOutputKeyClass(Text.class);
        enumJob.setOutputValueClass(IntWritable.class);

        enumJob.waitForCompletion(true);

        Job docCountJob = Job.getInstance(docCountConf, "Doc count Job");
        FileInputFormat.addInputPath(docCountJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(docCountJob, new Path(args[2]));

        docCountJob.setJarByClass(DocCount.class);
        docCountJob.setMapperClass(DocCount.Map.class);
        docCountJob.setReducerClass(DocCount.Map.class);

        docCountJob.setMapOutputKeyClass(IntWritable.class);
        docCountJob.setMapOutputValueClass(Text.class);
        docCountJob.setOutputKeyClass(Text.class);
        docCountJob.setOutputValueClass(IntWritable.class);

        docCountJob.waitForCompletion(true);

        Job indexerJob = Job.getInstance(enumeratorConf, "Indexer Job");
        FileInputFormat.addInputPath(indexerJob, new Path(args[2]));
        FileOutputFormat.setOutputPath(indexerJob, new Path(args[3]));

        indexerJob.setJarByClass(Indexer.class);
        indexerJob.setMapperClass(Indexer.Map.class);
        indexerJob.setReducerClass(Indexer.Reduce.class);

        indexerJob.setMapOutputKeyClass(Text.class);
        indexerJob.setMapOutputValueClass(IntWritable.class);
        indexerJob.setOutputKeyClass(Text.class);
        indexerJob.setOutputValueClass(IntWritable.class);

        indexerJob.waitForCompletion(true);
        // TODO: rm tmp outputs
        // TODO: change arg parsing
        // TODO: add required classes
    }
}
