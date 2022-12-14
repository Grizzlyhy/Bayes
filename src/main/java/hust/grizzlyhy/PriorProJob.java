package hust.grizzlyhy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class PriorProJob extends Configured implements Tool {

    public static class PriorProMap extends Mapper<Text, Text, Text, LongWritable> {
        //        private Text newKey = new Text();
        private final static LongWritable one = new LongWritable(1);

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String k2 = key.toString();
            String type = k2.split("_")[0];
            context.write(new Text(type), one);
        }
    }

    public static class PriorProReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        Configuration conf =getConf();
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem == null) {

        }
        FileSystem fs = FileSystem.get(new URI(FilePathBean.getOutputPriorPath()), conf, "LYP");
        fs.delete(new Path(FilePathBean.getOutputPriorPath()));
        fs.close();
        Job job = Job.getInstance(conf);
        //  ??????????????????(????????????????????????????????????)
        FileInputFormat.setInputPaths(job, new Path(FilePathBean.getTrainDataPath()));
        //  ??????????????????(????????????????????????????????????)
        FileOutputFormat.setOutputPath(job, new Path(FilePathBean.getOutputPriorPath()));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(PriorProMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(PriorProReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //  ??????job
        //  ????????????????????????????????????????????????????????????????????????WordCountJob?????????
        job.setJarByClass(PriorProMap.class);
        job.waitForCompletion(true);

        return 0;
    }


//    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//
//        //  job??????????????????
//        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://sf01:8020");
//        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//        conf.set("dfs.replication", "2");
//        System.setProperty("HADOOP_USER_NAME", "LYP");
//
////            FileSystem fs=FileSystem.get(new URI("hdfs://sf01:8020"),conf,"LYP");
////            fs.delete(new Path("/output"));
////            fs.close();
//        //  ????????????job
//        Job job = Job.getInstance(conf);
//        //  ??????????????????(????????????????????????????????????)
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        //  ??????????????????(????????????????????????????????????)
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setMapperClass(PriorProMap.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(LongWritable.class);
//        job.setReducerClass(PriorProReduce.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(LongWritable.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        //  ??????job
//        //  ????????????????????????????????????????????????????????????????????????WordCountJob?????????
//        job.setJarByClass(PriorProMap.class);
//        job.waitForCompletion(true);
//
//
//    }

}
