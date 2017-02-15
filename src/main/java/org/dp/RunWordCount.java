package org.dp;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class RunWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment
                .createRemoteEnvironment("192.168.1.100", 6123, "C:\\dev\\ideaWorkspace\\flink-hbase-exercises\\target\\flink-hbase-exercises-1.0-SNAPSHOT.jar");

        DataSource<Tuple2<String, String>> input = env.createInput(new TableInputFormat<Tuple2<String, String>>() {
            @Override
            protected Scan getScanner() {
                Scan result = new Scan();
                return result;
            }

            @Override
            protected String getTableName() {
                return "test-input";
            }

            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {
                Tuple2<String, String> tuple = new Tuple2<String, String>();
                tuple.setField(Bytes.toString(result.getRow()), 0);
                tuple.setField(Bytes.toString(result.getValue("d".getBytes(), "d".getBytes())), 1);
                return tuple;
            }
        });

        DataSet<Tuple2<String, Integer>> data =
                input.flatMap(new CopyFunction())
                .groupBy(0)
                .sum(1);

        // emit result
        Job job = Job.getInstance();
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "test-output");
        // TODO is "mapred.output.dir" really useful?
//        job.getConfiguration().set("mapred.output.dir",HBaseFlinkTestConstants.TMP_DIR);
        data.map(new RichMapFunction<Tuple2<String,Integer>, Tuple2<Text,Mutation>>() {
            private transient Tuple2<Text, Mutation> reuse;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                reuse = new Tuple2<Text, Mutation>();
            }

            @Override
            public Tuple2<Text, Mutation> map(Tuple2<String, Integer> t) throws Exception {
                reuse.f0 = new Text(t.f0);
                Put put = new Put(t.f0.getBytes());
                put.addColumn("d".getBytes(), "e".getBytes(), Bytes.toBytes(t.f1));
                reuse.f1 = put;
                return reuse;
            }
        }).output(new HadoopOutputFormat<Text, Mutation>(new TableOutputFormat<Text>(), job));
        env.execute("1 app");
    }

    public static class CopyFunction implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>> {

        @Override
        public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, Integer>> out) {

            Tuple2<String, Integer> tuple2 = new Tuple2<>();
            tuple2.setField(value.f0, 0);
            tuple2.setField(value.f1.length(), 1);
            out.collect(tuple2);
        }
    }
}
