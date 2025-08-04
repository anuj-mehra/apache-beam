package com.poc.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.net.HttpURLConnection;
import java.net.URL;

public class PartitionedHttpRequests {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Step 1: Read URLs from a file
        PCollection<String> urls = pipeline.apply("ReadURLs", TextIO.read().from("gs://your-bucket/path/url_list.txt"));

        // Step 2: Split into 10 partitions using Partition.of()
        PCollectionList<String> partitions = urls.apply("PartitionInto10", Partition.of(10, (PartitionFn<String>) (url, numPartitions) ->
                Math.abs(url.hashCode()) % numPartitions
        ));

        // Step 3: Process each partition in parallel
        for (int i = 0; i < 10; i++) {
            PCollection<String> part = partitions.get(i);
            final int partitionId = i;

            part.apply("ProcessPartition_" + i, ParDo.of(new DoFn<String, Void>() {
                @ProcessElement
                public void processElement(@Element String url) {
                    try {
                        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                        conn.setRequestMethod("GET");
                        conn.setConnectTimeout(3000);
                        conn.setReadTimeout(3000);
                        int responseCode = conn.getResponseCode();
                        System.out.println("Partition " + partitionId + ": " + url + " => " + responseCode);
                        conn.disconnect();
                    } catch (Exception e) {
                        System.err.println("Partition " + partitionId + " Error: " + url);
                        e.printStackTrace();
                    }
                }
            }));
        }

        pipeline.run().waitUntilFinish();
    }
}
