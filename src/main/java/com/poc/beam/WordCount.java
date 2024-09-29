package com.poc.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        PCollection<KV<String, Long>> wordCount = p
                .apply("(1) Read all lines",
                        TextIO.read().from("/Users/anujmehra/git/apache-beam/src/main/resources/input.txt"))
                .apply("(2) Flatmap to a list of words",
                        FlatMapElements.into(TypeDescriptors.strings())
                                .via(line -> Arrays.asList(line.split("\\s"))))
                .apply("(3) Lowercase all",
                        MapElements.into(TypeDescriptors.strings())
                                .via(word -> word.toLowerCase()))
                /*.apply("(4) Trim punctuations",
                        MapElements.into(TypeDescriptors.strings())
                                .via(word -> trim(word)))
                .apply("(5) Filter stopwords",
                        Filter.by(word -> !isStopWord(word)))*/
                .apply("(4) Count words",
                        Count.perElement());


        wordCount.apply(MapElements.into(TypeDescriptors.strings())
                .via(count -> count.getKey() + " --> " + count.getValue()))
                .apply(TextIO.write().to("/Users/anujmehra/git/apache-beam/src/main/resources/outputFile.txt"));

        p.run().waitUntilFinish();

    }
}
