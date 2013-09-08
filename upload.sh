#/bin/bash


me=$(hostname)
for i in `find ./dfs -type f`; do
  f=$(echo $i | sed 's/.\/dfs//' | sed "s/\.log/\.proxyby\.${me}.log/")
  echo hdfs dfs -copyFromLocal $i $f
  hdfs dfs -copyFromLocal $i $f
done
