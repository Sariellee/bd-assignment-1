package NaiveSearch.Query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query {
    private static final String outIndexer = "IndexerOut";
    private static final String outAnalyzer = "AnalyzerOut";
    private static final String outQuery = "QueryOut";

    public static void main(String[] args) throws Exception {
        Configuration relevanceAnalyzer = new Configuration();
        Configuration contentExtractorConf = new Configuration();
        FileSystem fs = FileSystem.get(relevanceAnalyzer);

        Job analyzerJob = Job.getInstance(relevanceAnalyzer, "Query Analyzer Job");
        FileInputFormat.addInputPath(analyzerJob, new Path(outIndexer));
        FileOutputFormat.setOutputPath(analyzerJob, new Path(outAnalyzer));

        analyzerJob.setJarByClass(QueryAnalyzer.class);
        analyzerJob.setMapperClass(QueryAnalyzer.MapJob.class);
        analyzerJob.setReducerClass(QueryAnalyzer.ReduceJob.class);
        if (fs.exists(new Path(outAnalyzer))) {
            fs.delete(new Path(outAnalyzer), true);
        }

        analyzerJob.setMapOutputKeyClass(Text.class);
        analyzerJob.setMapOutputValueClass(IntWritable.class);
        analyzerJob.setOutputKeyClass(Text.class);
        analyzerJob.setOutputValueClass(IntWritable.class);

//        analyzerJob.waitForCompletion(true);

        String Relevance = "12\t1\n" +
                "25\t0.5\n" +
                "39\t0.25\n";
        contentExtractorConf.setStrings("relevance", Relevance);
        Job contentExtractorJob = Job.getInstance(contentExtractorConf, "Content Extractor Job");
        FileInputFormat.addInputPath(contentExtractorJob, new Path("input"));
        FileOutputFormat.setOutputPath(contentExtractorJob, new Path(outQuery));
        if (fs.exists(new Path(outQuery))) {
            fs.delete(new Path(outQuery), true);
        }
        contentExtractorJob.setJarByClass(ContentExtractor.class);
        contentExtractorJob.setMapperClass(ContentExtractor.MapJob.class);
        contentExtractorJob.setReducerClass(ContentExtractor.ReduceJob.class);

        contentExtractorJob.setMapOutputKeyClass(DoubleWritable.class);
        contentExtractorJob.setMapOutputValueClass(Text.class);
        contentExtractorJob.setOutputKeyClass(DoubleWritable.class);
        contentExtractorJob.setOutputValueClass(Text.class);

        contentExtractorJob.waitForCompletion(true);

        //TODO: add respective classes
        //TODO: clean tmp output
    }
}
