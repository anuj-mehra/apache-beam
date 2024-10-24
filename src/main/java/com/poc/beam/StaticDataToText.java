package com.poc.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class StaticDataToText {

    public static void main(String[] args) {
        final PipelineOptions options = PipelineOptionsFactory.create();
        final Pipeline p = Pipeline.create(options);

        final List<String> input = Arrays.asList("first", "second", "third", "fourth", "fifth");

        final PCollection<String> pCollection = p.apply(Create.of(input));
        pCollection.apply(TextIO.write().to("/Users/anujmehra/git/apache-beam/src/main/resources/StaticDataToText/part-file/").withSuffix(".txt"));

        p.run().waitUntilFinish();
    }
}
