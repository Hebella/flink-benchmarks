package org.apache.flink.benchmark.flush;

import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.benchmark.BenchmarkBase;
import org.apache.flink.benchmark.FlinkEnvironmentContext;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class AllowedLatencyBenchmark extends BenchmarkBase {
    public static final int RECORDS_PER_INVOCATION = 5_000_000;
    public static final long CHECKPOINT_INTERVAL_MS = 3000;
    public static final long FLUSH_INTERVAL_MS = 0;
    public static final int KEY_SIZE = 50_000;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + AllowedLatencyBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    @OperationsPerInvocation(value = AllowedLatencyBenchmark.RECORDS_PER_INVOCATION)
    public void aggregationSink(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
        env.getConfig().setAllowedLatency(FLUSH_INTERVAL_MS);
        env.setParallelism(1);

        DataStream<Integer> ds1 = env.addSource(new MyAggregationSource(
                RECORDS_PER_INVOCATION,
                0,
                KEY_SIZE));
        ds1.keyBy(value -> value)
                .transform(
                        "MyAggregator",
                        new TupleTypeInfo<>(
                                IntegerTypeInfo.INT_TYPE_INFO, IntegerTypeInfo.LONG_TYPE_INFO),
                        new MyAggregator(value -> value))
                .addSink(new CountingAndDiscardingSink<>());

        env.execute();
    }
}
