package com.poc.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class ReadTextFile {

    public static void main(String[] args) {
        final PipelineOptions options = PipelineOptionsFactory.create();
        final Pipeline p = Pipeline.create(options);

        final PCollection<String> posnData = p.apply(TextIO.read().from("/Users/anujmehra/git/apache-beam/src/main/resources/StaticDataToTextOverwriteFile/29-Sep-24/posn/*"));

        posnData.apply(TextIO.write().to("/Users/anujmehra/git/apache-beam/src/main/resources/StaticDataToTextOverwriteFile/28-Sep-24/posn/")
                .withSuffix(".txt")
                .withDelimiter("|".toCharArray()));

        final PCollection<String> tranData = p.apply(TextIO.read().from("/Users/anujmehra/git/apache-beam/src/main/resources/StaticDataToTextOverwriteFile/29-Sep-24/tran/*"));

        tranData.apply(TextIO.write().to("/Users/anujmehra/git/apache-beam/src/main/resources/StaticDataToTextOverwriteFile/28-Sep-24/tran/")
                .withSuffix(".txt")
                .withDelimiter("|".toCharArray()));

        p.run().waitUntilFinish();
    }
}
