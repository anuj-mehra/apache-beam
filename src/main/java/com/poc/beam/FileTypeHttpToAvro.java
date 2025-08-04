package com.poc.beam;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class FileTypeHttpToAvro {

    public static void main(String[] args) throws Exception {

        Pipeline pipeline = Pipeline.create();

        // Load AVRO schema
        Schema schema = new Schema.Parser().parse(new File("schemas/my_record.avsc"));

        // Step 1: Read FileType|URL lines
        PCollection<String> lines = pipeline.apply("ReadInputFile",
                TextIO.read().from("gs://your-bucket/path/to/input.dat"));

        // Step 2: Parse into KV<FileType, URL>
        PCollection<KV<String, String>> fileTypeUrlPairs = lines.apply("ParseFileTypeAndUrl", ParDo.of(new DoFn<String, KV<String, String>>() {
            @ProcessElement
            public void processElement(@Element String line, OutputReceiver<KV<String, String>> out) {
                String[] parts = line.split("\\|", 2);
                if (parts.length == 2) {
                    out.output(KV.of(parts[0].trim(), parts[1].trim()));
                }
            }
        }));

        // Step 3: Split by FileType and process
        PCollection<GenericRecord> positionRecords = processUrlsByFileType(
                fileTypeUrlPairs.apply(Filter.by(kv -> kv.getKey().equalsIgnoreCase("Position"))),
                "Position", schema);

        PCollection<GenericRecord> accountRecords = processUrlsByFileType(
                fileTypeUrlPairs.apply(Filter.by(kv -> kv.getKey().equalsIgnoreCase("Account"))),
                "Account", schema);

        // Step 4: Write each FileType's output to Avro
        positionRecords.apply("WritePositionAvro", AvroIO.writeGenericRecords(schema)
                .to("gs://your-bucket/output/position")
                .withSuffix(".avro")
                .withNumShards(1));

        accountRecords.apply("WriteAccountAvro", AvroIO.writeGenericRecords(schema)
                .to("gs://your-bucket/output/account")
                .withSuffix(".avro")
                .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

    private static PCollection<GenericRecord> processUrlsByFileType(
            PCollection<KV<String, String>> urls,
            String fileType,
            Schema schema) {

        return urls.apply("Fetch_" + fileType, ParDo.of(new DoFn<KV<String, String>, GenericRecord>() {
            @ProcessElement
            public void processElement(@Element KV<String, String> element, OutputReceiver<GenericRecord> out) {
                String url = element.getValue();
                try {
                    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                    conn.setRequestMethod("GET");
                    conn.setConnectTimeout(5000);
                    conn.setReadTimeout(5000);

                    int status = conn.getResponseCode();
                    if (status == 200) {
                        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                        StringBuilder json = new StringBuilder();
                        String line;
                        while ((line = in.readLine()) != null) {
                            json.append(line);
                        }
                        in.close();
                        conn.disconnect();

                        // Convert JSON to GenericRecord
                        try {
                            JsonObject jsonObject = JsonParser.parseString(json.toString()).getAsJsonObject();
                            GenericRecord record = new GenericData.Record(schema);

                            // Example: assuming schema fields are "id", "name", "amount"
                            if (jsonObject.has("id")) record.put("id", jsonObject.get("id").getAsString());
                            if (jsonObject.has("name")) record.put("name", jsonObject.get("name").getAsString());
                            if (jsonObject.has("amount")) record.put("amount", jsonObject.get("amount").getAsDouble());

                            out.output(record);

                        } catch (Exception e) {
                            System.err.println("Invalid JSON or schema mismatch: " + url);
                        }

                    } else {
                        System.err.println("[" + fileType + "] HTTP error " + status + " from: " + url);
                    }

                } catch (Exception e) {
                    System.err.println("[" + fileType + "] Error fetching: " + url);
                    e.printStackTrace();
                }
            }
        }));
    }
}

