package NaiveSearch.Indexer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class TermDocs implements WritableComparable<TermDocs> {
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
