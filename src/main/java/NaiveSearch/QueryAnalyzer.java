package NaiveSearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class QueryAnalyzer {
    public static void main(String[] args) throws Exception {
        Configuration relevanceAnalizatorConf = new Configuration();
        Configuration contentExtractorConf = new Configuration();

        Job analizatorJob = Job.getInstance(relevanceAnalizatorConf, "Relevance Analizator Job");
        FileInputFormat.addInputPath(analizatorJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(analizatorJob, new Path(args[1]));

        analizatorJob.setJarByClass(RelevanceAnalizator.class);
        analizatorJob.setMapperClass(RelevanceAnalizator.Map.class);
        analizatorJob.setReducerClass(RelevanceAnalizator.Reduce.class);

        analizatorJob.setMapOutputKeyClass(Text.class);
        analizatorJob.setMapOutputValueClass(IntWritable.class);
        analizatorJob.setOutputKeyClass(Text.class);
        analizatorJob.setOutputValueClass(IntWritable.class);

        analizatorJob.waitForCompletion(true);

        Job contentExtractorJob = Job.getInstance(contentExtractorConf, "Content Extractor Job");
        FileInputFormat.addInputPath(contentExtractorJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(contentExtractorJob, new Path(args[2]));

        contentExtractorJob.setJarByClass(ContentExtractor.class);
        contentExtractorJob.setMapperClass(ContentExtractor.Map.class);
        contentExtractorJob.setReducerClass(ContentExtractor.Map.class);

        contentExtractorJob.setMapOutputKeyClass(IntWritable.class);
        contentExtractorJob.setMapOutputValueClass(Text.class);
        contentExtractorJob.setOutputKeyClass(Text.class);
        contentExtractorJob.setOutputValueClass(IntWritable.class);

        contentExtractorJob.waitForCompletion(true);

        //TODO: add respective classes
        //TODO: clean tmp output
    }
}
