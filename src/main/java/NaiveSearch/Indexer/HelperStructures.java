package NaiveSearch.Indexer;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

    public void set(DoubleWritable ifidf, Text term) {
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

class DocCount implements Writable {
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

    public void set(IntWritable count, IntWritable docId) {
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
class TermDocs implements WritableComparable<TermDocs> {
    private Text term;
    private IntWritable docId;

    public TermDocs(){
        this.term = new Text();
        this.docId = new IntWritable();
    }

    public TermDocs(IntWritable docId, Text term) {
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

