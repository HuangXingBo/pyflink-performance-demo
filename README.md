# pyflink-performance-demo

## flink 自身性能对比

1. 生成kafka的数据

```shell
python python/utils/prepare_data_log.py
```

2. 往kafka里面发数据
前置步骤是把python/utils/env.sh里面的kafka路径换成你的

```shell
./prepare_env.sh
```

3. 运行脚本run_flink_test.sh
前置步骤是配只好es，kibana等步骤（有点繁琐）

```shell
./run_flink_test.sh
```




## pyflink 1.11 和 pyspark 3.0 python UDF性能对比
前置步骤是在java目录下执行mvn clean package

```shell
./run_flink_perf_test.sh

./run_spark_perf_test.sh
```
