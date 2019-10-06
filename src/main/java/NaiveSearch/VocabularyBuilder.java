package NaiveSearch;
import javafx.util.Pair;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class VocabularyBuilder {
        public static void main(String[] args)throws Exception
        {

            File enumerator = new File("WordEnumerator.out/part-r-00000");
            File counter = new File("DocumentCount.out/part-r-00000");
            File out = new File("vocabulary");
            BufferedReader br_enum = new BufferedReader(new FileReader(enumerator));
            BufferedReader br_counter = new BufferedReader(new FileReader(counter));
            BufferedWriter voc = new BufferedWriter(new FileWriter(out));
            String st;
//            int ind;
//            String key;
            String [] arr;
            String [] arr2;
            Map<String,String> map1 = new HashMap<String, String>();
            Map< String, String> map2 = new HashMap<String, String>();

//            Pair<String, String> pair = new Pair<String, String>();


            while ((st = br_enum.readLine()) != null){
                arr = st.split("\\s+");
//                map.put(arr[0], arr[1]);
//                map.put(new Pair<String, String>(arr[0], arr[1]), "0");
                map1.put(arr[0], arr[1]);
//                System.out.println(arr[0]);
//                System.out.println(arr[1]);
            }
            while ((st = br_counter.readLine()) != null){
                arr = st.split("\\s+");
//                map.put(arr[0], arr[1]);
//                map.put(new Pair<String, String>(arr[0], arr[1]), "0");
//                map2.put(arr[0], arr[1]);
                voc.write( map1.get(arr[0])+" "+ arr[0] + " "+ arr[1] + "\n");
//                System.out.println(arr[0]);
//                System.out.println(arr[1]);
            }
            voc.close();
            br_enum.close();
            br_counter.close();




        }
    }

