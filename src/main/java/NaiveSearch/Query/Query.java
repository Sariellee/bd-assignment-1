package NaiveSearch.Query;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.util.StringTokenizer;

public class Query {
    private static final String outIndexer = "IndexerOut";
    private static final String outAnalyzer = "AnalyzerOut";
    private static final String outQuery = "QueryOut";
    public static final String usage ="" +
            "Usage: Query your_query max_documents [OPTIONS [PARAMS]]\n" +
            "YourQuery - query on which the search engine will search\n" +
            "MaxDocuments - maximum documents to show in rankings\n" +
            "OPTIONS:\n"+
            "--no-cleanup - do not remove intermediate results";
    public static final String[] options = {"--no-cleanup"};

    public static void main(String[] args) throws Exception {
        boolean cleanup = true;
        if (args.length < 2){
            System.out.println(usage);
            System.exit(1);
        }
        String query = args[0];
        int maxDocuments = Integer.parseInt(args[1]);
        JSONObject json = new JSONObject();
        StringTokenizer itr = new StringTokenizer(query.toLowerCase().replaceAll("[^a-z\\- ]", ""));
        while (itr.hasMoreTokens()) {
            String s = itr.nextToken();
            if (json.has(s)){
                json.put(s, json.getInt(s)+1);
            } else{
                json.put(s, 1);
            }
        }

        for (int i = 2; i <args.length ; i++) {
            if (args[i].equals(options[0])){
                cleanup = false;
            } else{
                System.out.println("Unknown argument: "+args[i]+"\n");
                System.out.println(usage);
                System.exit(1);
            }
        }

        Configuration relevanceAnalyzerConf = new Configuration();
        Configuration contentExtractorConf = new Configuration();
        FileSystem fs = FileSystem.get(relevanceAnalyzerConf);
        relevanceAnalyzerConf.set("query", json.toString());
        relevanceAnalyzerConf.setInt("max", maxDocuments);
        FileStatus[] status = fs.listStatus(new Path(outIndexer));
        String doc_files = "input";

        for (FileStatus stat: status){
            if (stat.getPath().getName().startsWith("dump_of_")){
                doc_files = stat.getPath().getName().replace("dump_of_", "");
            }
        }

        Job analyzerJob = Job.getInstance(relevanceAnalyzerConf, "Query Analyzer Job");

        FileInputFormat.addInputPath(analyzerJob, new Path(outIndexer));
        FileOutputFormat.setOutputPath(analyzerJob, new Path(outAnalyzer));

        analyzerJob.setJarByClass(QueryAnalyzer.class);
        analyzerJob.setMapperClass(QueryAnalyzer.MapJob.class);
        analyzerJob.setReducerClass(QueryAnalyzer.ReduceJob.class);
        if (fs.exists(new Path(outAnalyzer))) {
            fs.delete(new Path(outAnalyzer), true);
        }

        analyzerJob.setMapOutputKeyClass(DoubleWritable.class);
        analyzerJob.setMapOutputValueClass(IntWritable.class);
        analyzerJob.setSortComparatorClass(ContentExtractor.ReverseDoubleComparator.class);
        analyzerJob.setOutputKeyClass(IntWritable.class);
        analyzerJob.setOutputValueClass(DoubleWritable.class);


        analyzerJob.waitForCompletion(true);

        FSDataInputStream analyze = fs.open(new Path(outAnalyzer + "/part-r-00000"));
        String relevance= IOUtils.toString(analyze, "UTF-8");
        contentExtractorConf.setStrings("relevance", relevance);
        Job contentExtractorJob = Job.getInstance(contentExtractorConf, "Content Extractor Job");

        FileInputFormat.addInputPath(contentExtractorJob, new Path(doc_files));
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
        if (cleanup){
            if (fs.exists(new Path(outAnalyzer))) {
                fs.delete(new Path(outAnalyzer), true);
            }
        }
        FSDataInputStream file = fs.open(new Path(outQuery + "/part-r-00000"));
        String out= IOUtils.toString(file, "UTF-8");
        System.out.println("Result of the query is:");
        System.out.println(out);

    }
}
