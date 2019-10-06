package NaiveSearch.Indexer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class IFIDFJob{
    public static class MapJob extends Mapper<Object, Text, IntWritable, WordIFIDF> {

        private WordIFIDF wc = new WordIFIDF();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t");
            String[] word_tokens = tokens[0].split("=");
            double tfidf = (double) Integer.parseInt(tokens[2])/Integer.parseInt(word_tokens[1]);
            wc.set(new DoubleWritable(tfidf),new Text(word_tokens[0]));
            context.write(new IntWritable(Integer.parseInt(tokens[1])),wc);

        }
    }

    public static class ReduceJob extends Reducer<IntWritable, WordIFIDF, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<WordIFIDF> values, Context context) throws IOException, InterruptedException {
            JSONObject json = new JSONObject();
            for (WordIFIDF val : values) {
                json.put(val.getTerm().toString(), val.getIfidf().get());
            }
            context.write(key, new Text(json.toString()));
        }
    }


}

class WordIFIDF implements Writable {
    private Text term;
    private DoubleWritable ifidf;

    WordIFIDF() {
        this.term = new Text();
        this.ifidf = new DoubleWritable();
    }

    WordIFIDF(DoubleWritable count, Text term) {
        this.term = term;
        this.ifidf = count;
    }

    public Text getTerm() {
        return term;
    }

    public DoubleWritable getIfidf() {
        return ifidf;
    }

    public void setIfidf(DoubleWritable ifidf) {
        this.ifidf = ifidf;
    }

    public void setTerm(Text term) {
        this.term = term;
    }

    public void set(DoubleWritable ifidf, Text term){
        this.ifidf = ifidf;
        this.term = term;
    }


    public void readFields(DataInput in) throws IOException {
        term.readFields(in);
        ifidf.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        term.write(out);
        ifidf.write(out);
    }

    @Override
    public String toString() {
        return term.toString() + "\t" + ifidf.toString();
    }
}