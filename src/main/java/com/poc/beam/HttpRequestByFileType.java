package com.poc.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.net.HttpURLConnection;
import java.net.URL;

public class HttpRequestByFileType {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        // Step 1: Read .dat file (format: FileType|URL)
        PCollection<String> lines = pipeline.apply("ReadFile", TextIO.read().from("gs://your-bucket/path/to/input.dat"));

        // Step 2: Parse lines into KV<FileType, URL>
        PCollection<KV<String, String>> fileTypeToUrl = lines.apply("ParseLines", ParDo.of(new DoFn<String, KV<String, String>>() {
            @ProcessElement
            public void processElement(@Element String line, OutputReceiver<KV<String, String>> out) {
                String[] parts = line.split("\\|", 2);
                if (parts.length == 2) {
                    String fileType = parts[0].trim();
                    String url = parts[1].trim();
                    out.output(KV.of(fileType, url));
                } else {
                    System.err.println("Skipping invalid line: " + line);
                }
            }
        }));

        // Step 3: Group URLs by FileType
        PCollection<KV<String, Iterable<String>>> groupedByFileType = fileTypeToUrl.apply("GroupByFileType", GroupByKey.create());

        // Step 4: Process each FileType group (in parallel) and send HTTP requests
        groupedByFileType.apply("SendRequestsByFileType", ParDo.of(new DoFn<KV<String, Iterable<String>>, Void>() {
            @ProcessElement
            public void processElement(@Element KV<String, Iterable<String>> element) {
                String fileType = element.getKey();
                Iterable<String> urls = element.getValue();

                for (String url : urls) {
                    try {
                        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
                        connection.setRequestMethod("GET");
                        connection.setConnectTimeout(5000);
                        connection.setReadTimeout(5000);
                        int responseCode = connection.getResponseCode();
                        System.out.println("FileType [" + fileType + "]: " + url + " => " + responseCode);
                        connection.disconnect();
                    } catch (Exception e) {
                        System.err.println("Error for FileType [" + fileType + "] URL: " + url);
                        e.printStackTrace();
                    }
                }
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}

