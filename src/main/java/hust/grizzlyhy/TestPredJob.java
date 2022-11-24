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

        //  job需要的配置参
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://sf01:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication", "2");
        System.setProperty("HADOOP_USER_NAME", "LYP");


        FileSystem fs = FileSystem.get(new URI(FilePathBean.getOutputTestPath()), conf, "LYP");
        fs.delete(new Path(FilePathBean.getOutputTestPath()));
        fs.close();
        //  创建一个job
        Job job = Job.getInstance(conf);
        //  指定输入路径(可以是文件，也可以是目录)
        FileInputFormat.setInputPaths(job, new Path(FilePathBean.getOutputTestprePath()));
        //  指定输出路径(只能指定一个不存在的目录)
        FileOutputFormat.setOutputPath(job, new Path(FilePathBean.getOutputTestPath()));


        job.setMapperClass(TestPredMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TestPredReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //  提交job
        //  注意：这一行必须设置，否则在集群中执行的是找不到WordCountJob这个类
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
        //  创建一个job
        Job job = Job.getInstance(conf);
        //  指定输入路径(可以是文件，也可以是目录)
        FileInputFormat.setInputPaths(job, new Path(FilePathBean.getOutputTestprePath()));
        //  指定输出路径(只能指定一个不存在的目录)
        FileOutputFormat.setOutputPath(job, new Path(FilePathBean.getOutputTestPath()));


        job.setMapperClass(TestPredMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TestPredReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //  提交job
        //  注意：这一行必须设置，否则在集群中执行的是找不到WordCountJob这个类
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
                getCondiPro(); //条件概率
                getPriorPro();
            } catch (URISyntaxException e) {

                throw new RuntimeException(e);
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] lineValues = value.toString().split("\\t");    //分词，按照空白字符切割
            int i = 0;
            if (lineValues.length == 3) {
                class_Name = lineValues[0];   //得到类名
                fileName = lineValues[1];
                i = i + 2;
            }
            //得到文件名

            for (Map.Entry<String, Double> entry : priorProbability.entrySet()) {
                String className = entry.getKey();

                newKey.set(class_Name + "\t" + fileName);//新的键值的key为<类明 文档名>
                double tempValue = Math.log(entry.getValue());//构建临时键值对的value为各概率相乘,转化为各概率取对数再相加

                String tempKey = className + "\t" + lineValues[i];//构建临时键值对<class_word>,在wordsProbably表中查找对应的概率
                if (conditionalProbability.containsKey(tempKey)) {
                    //如果测试文档的单词在训练集中出现过，则直接加上之前计算的概率
                    tempValue += Math.log(conditionalProbability.get(tempKey));
                } else {//如果测试文档中出现了新单词则加上之前计算新单词概率
                    tempValue += Math.log(conditionalProbability.get(className));
                }

                newValue.set(className + "\t" + tempValue);//新的键值的value为<类名  概率>
                context.write(newKey, newValue);//一份文档遍历在一个类中遍历完毕,则将结果写入文件,即<docName,<class  probably>>
            }
        }
    }

    public static class TestPredReduce extends Reducer<Text, Text, Text, Text> {
        Text newValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean flag = false;//标记,若第一次循环则先赋值,否则比较若概率更大则更新
            String tempClass = null;
            double tempProbably = 0.0;
            for (Text value : values) {
                String[] result = value.toString().split("\\s");
                String className = result[0];
                String probably = result[1];
                if (!flag) {//循环第一次
                    tempClass = className;//value.toString().substring(0, index);
                    tempProbably = Double.parseDouble(probably);
                    flag = true;
                } else {//否则当概率更大时就更新tempClass和tempProbably
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
