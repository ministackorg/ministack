// Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
package ministack.flinktest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Test job for MiniStack's Managed Flink data plane.
 *
 * Reads the "Test" runtime property group through KinesisAnalyticsRuntime, counts
 * upwards in checkpointed state, and writes {"message", "n"} records to the
 * Kinesis stream named by the "StreamName" property through the endpoint in
 * AWS_ENDPOINT_URL. A restore from a savepoint continues the count.
 *
 * Built the way AWS's getting-started project builds a job: Flink and
 * aws-kinesisanalytics-runtime are provided by the runtime, not bundled, so
 * the jar holds only this class. It needs no connector or AWS SDK.
 */
public class CounterJob {
    public static void main(String[] args) throws Exception {
        Map<String, Properties> groups = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties group = groups.get("Test");
        if (group == null) {
            throw new IllegalStateException("runtime property group Test is missing");
        }
        String stream = group.getProperty("StreamName");
        String message = group.getProperty("Message");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new Counter()).setParallelism(1)
            .addSink(new KinesisSink(stream, message)).setParallelism(1);
        env.execute("ministack-counter");
    }

    public static class Counter implements SourceFunction<Long>, CheckpointedFunction {
        private volatile boolean running = true;
        private long next;
        private transient ListState<Long> state;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(next);
                    next++;
                }
                Thread.sleep(250);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.update(java.util.List.of(next));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("next", Long.class));
            for (Long value : state.get()) {
                next = value;
            }
        }
    }

    public static class KinesisSink extends RichSinkFunction<Long> {
        private final String stream;
        private final String message;
        private transient HttpClient http;
        private transient String endpoint;
        private transient String region;
        private transient String accessKey;

        KinesisSink(String stream, String message) {
            this.stream = stream;
            this.message = message;
        }

        @Override
        public void open(Configuration parameters) {
            http = HttpClient.newHttpClient();
            endpoint = System.getenv("AWS_ENDPOINT_URL");
            region = System.getenv("AWS_REGION");
            accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        }

        @Override
        public void invoke(Long n, Context context) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            String record = mapper.createObjectNode().put("message", message).put("n", n).toString();
            String body = mapper.createObjectNode()
                .put("StreamName", stream)
                .put("PartitionKey", "counter")
                .put("Data", Base64.getEncoder().encodeToString(record.getBytes(StandardCharsets.UTF_8)))
                .toString();
            HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint))
                .header("Content-Type", "application/x-amz-json-1.1")
                .header("X-Amz-Target", "Kinesis_20131202.PutRecord")
                .header("Authorization", "AWS4-HMAC-SHA256 Credential=" + accessKey + "/20260101/" + region
                    + "/kinesis/aws4_request, SignedHeaders=host, Signature=0")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("PutRecord returned " + response.statusCode() + ": " + response.body());
            }
        }
    }
}
