package com.poc.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;

public class HttpParallelRequestPipeline {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Step 1: Read URLs from a file (one URL per line)
        PCollection<String> urls = pipeline.apply("ReadURLs", TextIO.read().from("gs://your-bucket/path/to/url_file.txt"));

        // Step 2: Assign each URL to one of 10 shards randomly
        PCollection<KV<Integer, String>> sharded = urls.apply("ShardURLs", ParDo.of(new DoFn<String, KV<Integer, String>>() {
            @ProcessElement
            public void processElement(@Element String url, OutputReceiver<KV<Integer, String>> out) {
                int shardId = Math.abs(url.hashCode()) % 10; // 10 shards
                out.output(KV.of(shardId, url));
            }
        }));

        // Step 3: Group by shard
        PCollection<KV<Integer, Iterable<String>>> groupedByShard = sharded.apply("GroupByShard", GroupByKey.create());

        // Step 4: Process each shard in parallel
        groupedByShard.apply("SendHttpRequestsInShard", ParDo.of(new DoFn<KV<Integer, Iterable<String>>, Void>() {
            @ProcessElement
            public void processElement(@Element KV<Integer, Iterable<String>> shard) {
                for (String url : shard.getValue()) {
                    try {
                        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
                        connection.setRequestMethod("GET");
                        connection.setConnectTimeout(5000);
                        connection.setReadTimeout(5000);
                        int responseCode = connection.getResponseCode();
                        System.out.println("Shard " + shard.getKey() + ": " + url + " => " + responseCode);
                        connection.disconnect();
                    } catch (Exception e) {
                        System.err.println("Error processing URL: " + url);
                        e.printStackTrace();
                    }
                }
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}

