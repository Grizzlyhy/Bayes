import os
import collections
dic = collections.OrderedDict()
for dirpath, dirnames, filenames in os.walk('F:\\04hadoop\\NBCorpus\\Country'):
    dic[dirpath] = len(filenames)
vd = collections.OrderedDict(sorted(dic.items(),key=lambda t:t[1]))
print(vd)