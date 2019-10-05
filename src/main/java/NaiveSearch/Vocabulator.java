package NaiveSearch;

import java.io.*;

public class Vocabulator {
    public static void main(String[] args) throws IOException {
        String input_file = "./output/part-r-00000";
        String output_file1 = "./output/vocabulary";
        String output_file2 = "./output/idf";


        BufferedReader reader = new BufferedReader(new FileReader(input_file));
        BufferedWriter writer1 = new BufferedWriter(new FileWriter(output_file1));
        BufferedWriter writer2 = new BufferedWriter(new FileWriter(output_file2));


        String currentLine = reader.readLine();
        for (int i = 0; currentLine != null; i ++){
            String[] spl = currentLine.split("\t");
            String word = spl[0];
            int idf = Integer.parseInt(spl[1]);
            currentLine = reader.readLine();

            writer1.write(String.valueOf(i) + " " + word + "\n");
            writer2.write(String.valueOf(i) + " " + String.valueOf(idf) + "\n");
        }


        reader.close();
    }
}
