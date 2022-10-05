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

选择合适的文件夹作为我们的测试用例，根据结果
