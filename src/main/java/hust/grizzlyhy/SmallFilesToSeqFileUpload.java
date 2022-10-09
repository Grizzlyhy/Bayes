package hust.grizzlyhy;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
public class SmallFilesToSeqFileUpload {

    /**
     *合并小文件，
     * @param inputDir
     * @param outputFile
     * @throws IOException
     */
    public static void write(String inputDir,String outputFile) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://sf01:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication", "2");
        System.setProperty("HADOOP_USER_NAME", "LYP");
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(new Path(outputFile), true);
        SequenceFile.Writer.Option[] opts = new SequenceFile.Writer.Option[]{
                SequenceFile.Writer.file(new Path(outputFile)),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(Text.class)
        };
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, opts);
        File inputDirPath = new File(inputDir);
        if (inputDirPath.isDirectory()) {
            File[] files = inputDirPath.listFiles();
            for (File file : files) {
                String content = FileUtils.readFileToString(file, "UTF-8");
                String title = file.getName();
                Text key = new Text(title);
                Text value = new Text(content);
                writer.append(key, value);
            }
        }
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        write("G:\\Bayes\\Bayes\\bayes_data\\test","/input/test");
        write("G:\\Bayes\\Bayes\\bayes_data\\train","/input/train");
    }

}
