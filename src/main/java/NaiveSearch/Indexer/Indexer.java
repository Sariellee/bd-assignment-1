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
    private static final String outCount = "outCountTMP";
    private static final String outIDF = "outIDFTMP";
    private static final String outIndexer = "IndexerOut";
    public static final String usage ="" +
            "Usage: Indexer <path_to_files> [OPTIONS [PARAMS]]\n" +
            "<path_to_files> - directory with files which will be indexed\n\n" +
            "OPTIONS:\n"+
            "--no-cleanup - do not remove intermediate results";
    private static final String[] options = {"--no-cleanup"};

    public static void main(String[] args) throws Exception {
        boolean cleanup = true;
        String skip_file = "";
        if (args.length == 0 || args[0].equals("--help")){
            System.out.println(usage);
            System.exit(1);
        }
        String input = args[0];
        Configuration WordCountJobConf = new Configuration();
        Configuration IDFJobConf = new Configuration();
        Configuration IFIDFJobConf = new Configuration();
        FileSystem fs = FileSystem.get(WordCountJobConf);
        if (!fs.exists(new Path(input))){
            System.out.println("Input directory doesn't exists");
            System.out.println(usage);
        }

        if (fs.exists(new Path(outCount))) {
            fs.delete(new Path(outCount), true);
        }

        Job wordCountJob = Job.getInstance(WordCountJobConf, "Count Words Job");
        FileInputFormat.addInputPath(wordCountJob, new Path(input));
        FileOutputFormat.setOutputPath(wordCountJob, new Path(outCount));

        for (int i = 1; i <args.length ; i++) {
            if (args[i].equals(options[0])){
                cleanup = false;
            } else{
                System.out.println("Unknown argument: "+args[i]+"\n");
                System.out.println(usage);
                System.exit(1);
            }
        }

        wordCountJob.setMapOutputKeyClass(TermDocs.class);
        wordCountJob.setMapOutputValueClass(IntWritable.class);
        wordCountJob.setOutputKeyClass(TermDocs.class);
        wordCountJob.setOutputValueClass(IntWritable.class);

        wordCountJob.setJarByClass(CountWords.class);
        wordCountJob.setMapperClass(CountWords.MapJob.class);
        wordCountJob.setReducerClass(CountWords.ReduceJob.class);


        wordCountJob.waitForCompletion(true);

        Job IDFJob = Job.getInstance(IDFJobConf, "IDF Job");
        FileInputFormat.addInputPath(IDFJob, new Path(outCount));
        FileOutputFormat.setOutputPath(IDFJob, new Path(outIDF));
        if (fs.exists(new Path(outIDF))) {
            fs.delete(new Path(outIDF), true);
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
        FileInputFormat.addInputPath(IFIDFJob, new Path(outIDF));
        FileOutputFormat.setOutputPath(IFIDFJob, new Path(outIndexer));
        if (fs.exists(new Path(outIndexer))) {
            fs.delete(new Path(outIndexer), true);
        }

        IFIDFJob.setMapOutputKeyClass(IntWritable.class);
        IFIDFJob.setMapOutputValueClass(WordIFIDF.class);
        IFIDFJob.setOutputKeyClass(IntWritable.class);
        IFIDFJob.setOutputValueClass(Text.class);

        IFIDFJob.setJarByClass(IFIDF.class);
        IFIDFJob.setMapperClass(IFIDF.MapJob.class);
        IFIDFJob.setReducerClass(IFIDF.ReduceJob.class);
        IFIDFJob.waitForCompletion(true);

        if (cleanup){
            if (fs.exists(new Path(outIDF))) {
                fs.delete(new Path(outIDF), true);
            }
            if (fs.exists(new Path(outCount))) {
                fs.delete(new Path(outCount), true);
            }
        }

    }
}
