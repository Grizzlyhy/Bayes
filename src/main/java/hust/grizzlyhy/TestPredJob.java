package hust.grizzlyhy;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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

public class TestPredJob extends Configured implements Tool {
    private static final HashMap<String, Double> priorProbability = new HashMap<String, Double>();
    private static final HashMap<String, Double> conditionalProbability = new HashMap<>();

    public static void getPriorPro() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://sf01:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication", "2");
        System.setProperty("HADOOP_USER_NAME", "LYP");

        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String line = null;
        FileSystem fs = FileSystem.get(conf);
        fsr = fs.open(new Path(FilePathBean.getOutputPriorPath() + "/part-r-00000"));
        bufferedReader = new BufferedReader(new InputStreamReader(fsr));
        HashMap<String, Double> temp = new HashMap<>();
        double sum = 0.0;
        while ((line = bufferedReader.readLine()) != null) {

            StringTokenizer tokenizer = new StringTokenizer(line);
            String className = tokenizer.nextToken();
            String number = tokenizer.nextToken();
            double numC = Double.parseDouble(number);
            sum += numC;
            temp.put(className, numC);
        }
        bufferedReader.close();
        for (Map.Entry<String, Double> val : temp.entrySet()) {
            String key = val.getKey();
            double value = Double.parseDouble(val.getValue().toString());
            value /= sum;
            priorProbability.put(key, value);
        }
    }

    public static void getCondiPro() throws URISyntaxException, IOException {

        FSDataInputStream fsr = null;
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://sf01:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication", "2");
        System.setProperty("HADOOP_USER_NAME", "LYP");

        BufferedReader bufferedReader = null;
        String line = null;
        FileSystem fs = FileSystem.get(new URI(FilePathBean.getOutputCondiPath() + "/part-r-00000"), conf);
        fsr = fs.open(new Path(FilePathBean.getOutputCondiPath() + "/part-r-00000"));
        bufferedReader = new BufferedReader(new InputStreamReader(fsr));
        HashMap<String, Double> wordSum = new HashMap<String, Double>();
        while ((line = bufferedReader.readLine()) != null) {
            StringTokenizer tokenizer = new StringTokenizer(line);
            String className = tokenizer.nextToken();
            String word = tokenizer.nextToken();
            line = bufferedReader.readLine();
            tokenizer = new StringTokenizer(line);
            String number = tokenizer.nextToken();
            Double numC = Double.parseDouble(number);
            if (wordSum.containsKey(className)) {
                wordSum.put(className, wordSum.get(className) + numC + 1.0);
            } else {
                wordSum.put(className, numC + 1.0);
            }
        }
        bufferedReader.close();
        fsr.close();

        fs = FileSystem.get(new URI(FilePathBean.getOutputCondiPath() + "/part-r-00000"), conf);
        fsr = fs.open(new Path(FilePathBean.getOutputCondiPath() + "/part-r-00000"));
        bufferedReader = new BufferedReader(new InputStreamReader(fsr));

        while ((line = bufferedReader.readLine()) != null) {
            StringTokenizer tokenizer = new StringTokenizer(line);
            String className = tokenizer.nextToken();
            String word = tokenizer.nextToken();
            line = bufferedReader.readLine();
            tokenizer = new StringTokenizer(line);
            String number = tokenizer.nextToken();
            Double numC = Double.parseDouble(number);
            String key = className + "\t" + word;
            conditionalProbability.put(key, (numC + 1.0) / wordSum.get(className));
        }
        bufferedReader.close();
        fsr.close();
        Iterator<Map.Entry<String, Double>> iterator = wordSum.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Double> entry = iterator.next();
            Object key = entry.getKey();
            conditionalProbability.put(key.toString(), 1.0 / Double.parseDouble(entry.getValue().toString()));
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        //  job??????????????????
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://sf01:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication", "2");
        System.setProperty("HADOOP_USER_NAME", "LYP");


        FileSystem fs = FileSystem.get(new URI(FilePathBean.getOutputTestPath()), conf, "LYP");
        fs.delete(new Path(FilePathBean.getOutputTestPath()));
        fs.close();
        //  ????????????job
        Job job = Job.getInstance(conf);
        //  ??????????????????(????????????????????????????????????)
        FileInputFormat.setInputPaths(job, new Path(FilePathBean.getOutputTestprePath()));
        //  ??????????????????(????????????????????????????????????)
        FileOutputFormat.setOutputPath(job, new Path(FilePathBean.getOutputTestPath()));


        job.setMapperClass(TestPredMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TestPredReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //  ??????job
        //  ????????????????????????????????????????????????????????????????????????WordCountJob?????????
        job.setJarByClass(TestPredJob.class);
        job.waitForCompletion(true);
        boolean result = job.waitForCompletion(true);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf =getConf();


        FileSystem fs = FileSystem.get(new URI(FilePathBean.getOutputTestPath()), conf, "LYP");
        fs.delete(new Path(FilePathBean.getOutputTestPath()));
        fs.close();
        //  ????????????job
        Job job = Job.getInstance(conf);
        //  ??????????????????(????????????????????????????????????)
        FileInputFormat.setInputPaths(job, new Path(FilePathBean.getOutputTestprePath()));
        //  ??????????????????(????????????????????????????????????)
        FileOutputFormat.setOutputPath(job, new Path(FilePathBean.getOutputTestPath()));


        job.setMapperClass(TestPredMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TestPredReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //  ??????job
        //  ????????????????????????????????????????????????????????????????????????WordCountJob?????????
        job.setJarByClass(TestPredJob.class);
        job.waitForCompletion(true);
        boolean result = job.waitForCompletion(true);
        return (result ? 0 : 1);
    }

    public static class TestPredMapper extends Mapper<LongWritable, Text, Text, Text> {

        String class_Name = "";
        String fileName = "";
        private final Text newKey = new Text();
        private final Text newValue = new Text();

        public void setup(Context context) throws IOException {

            try {
                getCondiPro(); //????????????
                getPriorPro();
            } catch (URISyntaxException e) {

                throw new RuntimeException(e);
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] lineValues = value.toString().split("\\t");    //?????????????????????????????????
            int i = 0;
            if (lineValues.length == 3) {
                class_Name = lineValues[0];   //????????????
                fileName = lineValues[1];
                i = i + 2;
            }
            //???????????????

            for (Map.Entry<String, Double> entry : priorProbability.entrySet()) {
                String className = entry.getKey();

                newKey.set(class_Name + "\t" + fileName);//???????????????key???<?????? ?????????>
                double tempValue = Math.log(entry.getValue());//????????????????????????value??????????????????,????????????????????????????????????

                String tempKey = className + "\t" + lineValues[i];//?????????????????????<class_word>,???wordsProbably???????????????????????????
                if (conditionalProbability.containsKey(tempKey)) {
                    //??????????????????????????????????????????????????????????????????????????????????????????
                    tempValue += Math.log(conditionalProbability.get(tempKey));
                } else {//???????????????????????????????????????????????????????????????????????????
                    tempValue += Math.log(conditionalProbability.get(className));
                }

                newValue.set(className + "\t" + tempValue);//???????????????value???<??????  ??????>
                context.write(newKey, newValue);//?????????????????????????????????????????????,????????????????????????,???<docName,<class  probably>>
            }
        }
    }

    public static class TestPredReduce extends Reducer<Text, Text, Text, Text> {
        Text newValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean flag = false;//??????,??????????????????????????????,????????????????????????????????????
            String tempClass = null;
            double tempProbably = 0.0;
            for (Text value : values) {
                String[] result = value.toString().split("\\s");
                String className = result[0];
                String probably = result[1];
                if (!flag) {//???????????????
                    tempClass = className;//value.toString().substring(0, index);
                    tempProbably = Double.parseDouble(probably);
                    flag = true;
                } else {//?????????????????????????????????tempClass???tempProbably
                    if (Double.parseDouble(probably) > tempProbably) {
                        tempClass = className;
                        tempProbably = Double.parseDouble(probably);
                    }
                }
            }
            newValue.set(tempClass + "\t" + tempProbably);
            //newValue.set(tempClass+":"+values.iterator().next());
            context.write(key, newValue);

        }
    }

}
