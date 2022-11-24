package hust.grizzlyhy;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import javax.crypto.spec.PSource;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestPreJob extends Configured implements Tool {



    public static class TestPreMapper extends Mapper<Text, Text, Text, Text> {
        private Text key_out = new Text();
        private Text value_out = new Text();
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = key.toString();
            String className = fileName.split("_")[0];
            key_out.set(className + "\t" +fileName);

            String[] split = value.toString().split("\n");
            for(String word:split){
                value_out.set(word);
                context.write(key_out, value_out);
            }
        }
    }

    public static class TestPreReduce extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();


            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                StringBuilder stringBuilder = new StringBuilder();

                for (Text value : values){
                    stringBuilder = stringBuilder.append(value.toString());
                }
                result.set(stringBuilder.toString());
                context.write(key, result);
            }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf =getConf();


        FileSystem fs = FileSystem.get(new URI(FilePathBean.getOutputTestprePath()), conf, "LYP");
        fs.delete(new Path(FilePathBean.getOutputTestprePath()));
        fs.close();
        //  创建一个job
        Job job = Job.getInstance(conf);
        //  指定输入路径(可以是文件，也可以是目录)
        FileInputFormat.setInputPaths(job, new Path(FilePathBean.getTestDataPath()));
        //  指定输出路径(只能指定一个不存在的目录)
        FileOutputFormat.setOutputPath(job, new Path(FilePathBean.getOutputTestprePath()));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(TestPreMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TestPreReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //  提交job
        //  注意：这一行必须设置，否则在集群中执行的是找不到WordCountJob这个类
        job.setJarByClass(TestPreJob.class);
        job.waitForCompletion(true);
        boolean result = job.waitForCompletion(true);
        return (result ? 0 : 1);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        //  job需要的配置参
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://sf01:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication", "2");
        System.setProperty("HADOOP_USER_NAME", "LYP");


        FileSystem fs = FileSystem.get(new URI(FilePathBean.getOutputTestprePath()), conf, "LYP");
        fs.delete(new Path(FilePathBean.getOutputTestprePath()));
        fs.close();
        //  创建一个job
        Job job = Job.getInstance(conf);
        //  指定输入路径(可以是文件，也可以是目录)
        FileInputFormat.setInputPaths(job, new Path(FilePathBean.getTestDataPath()));
        //  指定输出路径(只能指定一个不存在的目录)
        FileOutputFormat.setOutputPath(job, new Path(FilePathBean.getOutputTestprePath()));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(TestPreMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TestPreReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //  提交job
        //  注意：这一行必须设置，否则在集群中执行的是找不到WordCountJob这个类
        job.setJarByClass(TestPreJob.class);
        job.waitForCompletion(true);
        boolean result = job.waitForCompletion(true);
    }

}
