package hust.grizzlyhy;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

public class TestPreJob extends Configured implements Tool {
    private static HashMap<String, Double>priorProbability = new HashMap<String, Double>();
    private static HashMap<String, Double> conditionalProbability = new HashMap<>();

    public static void getPriorPro(Configuration conf) throws IOException, URISyntaxException {
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String line = null;
        FileSystem fs = FileSystem.get(conf);
        fsr = fs.open(new Path(FilePathBean.getOutputPriorPath() + "/part-r-00000"));
        bufferedReader = new BufferedReader(new InputStreamReader(fsr));
        HashMap<String, Double> temp = new HashMap<>();
        double sum=0.0;
        while ((line = bufferedReader.readLine()) != null) {
            System.out.println(line);
            StringTokenizer tokenizer = new StringTokenizer(line);
            String className = tokenizer.nextToken();
            String number = tokenizer.nextToken();
            double numC = Double.parseDouble(number);
            sum+=numC;
            temp.put(className, numC);
        }
        bufferedReader.close();
        for (Map.Entry<String, Double> val : temp.entrySet()) {
            String key = val.getKey().toString();
            double value = Double.parseDouble(val.getValue().toString());
            value /= sum;
            priorProbability.put(key, value);
        }
    }
    public static void getCondiPro(Configuration conf) throws URISyntaxException, IOException {
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String line = null;
        FileSystem fs = FileSystem.get(new URI(FilePathBean.getOutputCondiPath() + "/part-r-00000"), conf);
        fsr = fs.open(new Path(FilePathBean.getOutputCondiPath() + "/part-r-00000"));
        bufferedReader = new BufferedReader(new InputStreamReader(fsr));
        HashMap<String,Double> wordSum=new HashMap<String, Double>();
        while ((line = bufferedReader.readLine()) != null) {
            StringTokenizer tokenizer = new StringTokenizer(line);
            String className = tokenizer.nextToken();
            String word = tokenizer.nextToken();
            line = bufferedReader.readLine();
            tokenizer = new StringTokenizer(line);
            String number = tokenizer.nextToken();
            Double numC=Double.parseDouble(number);
            String key = className+"\t"+word;
            wordSum.put(key, numC);
            if(wordSum.containsKey(key))
                wordSum.put(key,wordSum.get(key)+numC+1.0);//加1.0是因为每一次都是一个不重复的单词
            else
                wordSum.put(key,numC+1.0);
        }
        }
        bufferedReader.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        //  job需要的配置参


        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://sf01:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication", "2");
        System.setProperty("HADOOP_USER_NAME", "LYP");
        getPriorPro(conf);
        getCondiPro(conf);
    }

    @Override
    public int run(String[] strings) throws Exception {

        return 0;
    }
}
