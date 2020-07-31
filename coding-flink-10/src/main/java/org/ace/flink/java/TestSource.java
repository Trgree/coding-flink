package org.ace.flink.java;

import org.ace.flink.source.AutoGenerateSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream lines = env.addSource(new AutoGenerateSource());

        lines.print("source");
        env.execute("testSource");
    }
}
