package com.wing.chap02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;

public class SecondarySortingJob {

    static String INPUT_FILENAME = "chap02/secondary_sorting_input.csv";
    static String OUTPUT_FILENAME = "chap02/secondary_sorting_output.csv";

    public static void main(String[] args) throws Exception {
        // 本地环境
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        // 准备参数(输出路径为 target/classes 下)
        String dir = SecondarySortingJob.class.getClassLoader().getResource("").getPath();
        String inputFilePath = dir+"/"+INPUT_FILENAME;
        String outputFilePath = dir+"/"+OUTPUT_FILENAME;

        // 准备数据
        DataSource<String> dataSource = env.readTextFile(inputFilePath);

        // 开始计算
        DataSet result = dataSource
                // 2000,12,04,10
                .map(new MapFunction<String, Tuple3<String, Integer, String>>() {
                    public Tuple3<String, Integer, String> map(String s) throws Exception {
                        String[] splited = s.split(",");
                        String yearMonth = splited[0] + "-" + splited[1];
                        Integer temperature = Integer.parseInt(splited[3]);
                        String day = splited[2];
                        return new Tuple3<String, Integer, String>(yearMonth, temperature, day);
                    }
                })
                .partitionByRange(1)
                .sortPartition(1, Order.ASCENDING)
                .map(new MapFunction<Tuple3<String, Integer, String>, Tuple2<String, String>>() {
                    public Tuple2<String, String> map(Tuple3<String, Integer, String> tuple3) throws Exception {
                        return new Tuple2<String, String>(tuple3.f0, tuple3.f1.toString());
                    }
                })
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple2<String, String>>() {
                    public Tuple2<String, String> reduce(Tuple2<String, String> t1, Tuple2<String,
                            String> t2) throws Exception {
                        return new Tuple2<String, String>(t1.f0, t1.f1 + "," + t2.f1);
                    }
                })
                ;

        // 单线程输出（确保是在一个文件中）
        result.writeAsText(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("SecondarySorting");

    }

}
