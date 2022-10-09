# 使用Mapreduce实现朴素贝叶斯分类器

- github:https://github.com/Grizzlyhy/Bayes
- csdn:https://blog.csdn.net/qq_43598681
- github 上面进行了代码托管
- csdn 介绍了一些方法

## 数据集的选取与上传

getFiles.py()

```python
import collections
dic = collections.OrderedDict()
for dirpath, dirnames, filenames in os.walk('F:\\04hadoop\\NBCorpus\\Country'):
    dic[dirpath] = len(filenames)
vd = collections.OrderedDict(sorted(dic.items(),key=lambda t:t[1]))
print(vd)
    
```

选择合适的文件夹作为我们的测试用例，根据结果选择(数量均衡  不是太少)以下两个文件夹作为我们数据集

```json
('F:\\04hadoop\\NBCorpus\\Country\\CHINA', 255), ('F:\\04hadoop\\NBCorpus\\Country\\GFR', 257)
```

- 将数据压缩成zip格式

- 利用moberxterm将数据压缩上传至sf01虚拟机中, /opt/download

- 安装unzip

  ```shell
  yum -y install unzip 
  ```

- 将文件解压至data目录下

  ```
  sudo unzip bayes_data.zip -d /opt/data/
  ```

  
