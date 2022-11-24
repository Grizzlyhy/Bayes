package hust.grizzlyhy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://sf01:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication", "2");
        System.setProperty("HADOOP_USER_NAME", "LYP");

// 本地执行运行代码
//        PreDatabase preDatabase = new PreDatabase();
//        ToolRunner.run(conf,preDatabase,args);
//        SmallFilesToSeqFileUpload smallFilesToSeqFileUpload = new SmallFilesToSeqFileUpload();
//        ToolRunner.run(conf,smallFilesToSeqFileUpload,args);


        PriorProJob priorProJob = new PriorProJob();
        ToolRunner.run(conf, priorProJob, args);
        ConditionalProJob conditionalProJob = new ConditionalProJob();
        ToolRunner.run(conf, conditionalProJob, args);
        TestPreJob testPreJob = new TestPreJob();
        ToolRunner.run(conf, testPreJob, args);
        TestPredJob testPredJob = new TestPredJob();
        ToolRunner.run(conf, testPredJob, args);
        Evaluation evaluation = new Evaluation();
        ToolRunner.run(conf, evaluation, args);




    }
}