package NaiveSearch;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class VocabularyBuilder {
        public static void main(String[] args)throws Exception
        {

            File enumerator = new File("WordEnumerator.out/part-r-00000");
            BufferedReader br_enum = new BufferedReader(new FileReader(enumerator));
            String st;
            int ind;
            String key;
            Map<String, Integer> map = new HashMap<String, Integer>();
            while ((st = br_enum.readLine()) != null){
//                map.add(st.split("\\s+", 0));
//                System.out.println(arrOfStr[0]);
            }


        }
    }

