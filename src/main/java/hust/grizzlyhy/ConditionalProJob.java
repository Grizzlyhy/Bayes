package hust.grizzlyhy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ConditionalProJob {

    public static class ConditionalProMap extends Mapper<Text, Text, Text, LongWritable> {
        //        private Text newKey = new Text();
        private final static LongWritable one = new LongWritable(1);

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String k2 = key.toString();
            String type = k2.split("_")[0];
//            System.out.println(k2.split("_")[1]);
            String[] split = value.toString().split("\n");
            for(String word:split){
                String key2=type+"\t"+word;
                context.write(new Text(key2), one);
            }


        }
    }

    public static class ConditionalProReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();
//        private StringBuffer stringBuffer=new StringBuffer();
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        //  job需要的配置参


        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://sf01:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication", "2");
        System.setProperty("HADOOP_USER_NAME", "LYP");


        FileSystem fs = FileSystem.get(new URI(args[1]), conf, "LYP");
        fs.delete(new Path(args[1]));
        fs.close();
        //  创建一个job
        Job job = Job.getInstance(conf);
        //  指定输入路径(可以是文件，也可以是目录)
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //  指定输出路径(只能指定一个不存在的目录)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(ConditionalProMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(ConditionalProReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //  提交job
        //  注意：这一行必须设置，否则在集群中执行的是找不到WordCountJob这个类
        job.setJarByClass(ConditionalProMap.class);
        job.waitForCompletion(true);


    }

}
