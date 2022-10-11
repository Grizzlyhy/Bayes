# 使用Mapreduce实现朴素贝叶斯分类器

- github:https://github.com/Grizzlyhy/Bayes
- csdn:https://blog.csdn.net/qq_43598681
- github 上面进行了代码托管
- csdn 介绍了一些方法

## 数据集的选取与上传

### 选择数据集

getFiles.py()

```python
import collections
dic = collections.OrderedDict()
for dirpath, dirnames, filenames in os.walk('F:\\04hadoop\\NBCorpus\\Country'):
    dic[dirpath] = len(filenames)
vd = collections.OrderedDict(sorted(dic.items(),key=lambda t:t[1]))
print(vd)
    
```

选择合适的文件夹作为我们的测试用例，根据结果选择(数量集不是太少)以下两个文件夹作为我们数据集

```json
('F:\\04hadoop\\NBCorpus\\Country\\CHINA', 255), ('F:\\04hadoop\\NBCorpus\\Country\\GFR', 257)
```

### 数据集的随机打乱，与分类上传

#### 打乱与分类

```java
public static void shuffle(String inputDir, String outputDir) {
        File inputDirPath = new File(inputDir);
        if (!inputDirPath.isDirectory()) {
            exit(1001);
        }
        File[] inputDirs = inputDirPath.listFiles();

        File trainPath = new File(outputDir + "\\train");
        if (!trainPath.exists()) {
            trainPath.mkdirs();
        }
        File testPath = new File(outputDir + "\\test");
        if (!testPath.exists()) {
            testPath.mkdirs();
        }
        for(int i=0;i<inputDirs.length;i++)
            selletTestSet(inputDir,outputDir,inputDirs[i].getName());
    }

    public static void selletTestSet(String inputDir, String outputDir, String type) {
        File typeDirPath = new File(inputDir + "\\" + type);
        File[] typeFiles = typeDirPath.listFiles();

        String trainPath = outputDir + "\\train";
        String testPath = outputDir + "\\test";

        Arrays.stream(typeFiles).unordered();
        int i = 0;
        int flag = typeFiles.length * 7 / 10;
        for (File f : typeFiles) {
            i++;
            if (i <= flag) {
                f.renameTo(new File(trainPath, type +"_"+ i + ".txt"));
            } else {
                f.renameTo(new File(testPath, type +"_"+ i + ".txt"));
            }
        }

    }

    public static void main(String[] args) {
        shuffle("G:\\Bayes\\Bayes\\data", "G:\\Bayes\\Bayes\\bayes_data");
    }
```

#### 数据集压缩与上传

利用idea 将**小文件**压缩为**seqFile** 在上传到hdfs集群

```java
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
```



