package com.poc.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.Arrays;
import java.util.List;

public class UnionTwoCollections {

    public static void main(String[] args) {
        final PipelineOptions options = PipelineOptionsFactory.create();
        final Pipeline p = Pipeline.create(options);

        final List<String> input1 = Arrays.asList("first", "second", "third", "fourth", "fifth", "sixth");
        final List<String> input2 = Arrays.asList("sixth", "seventh", "eighth", "ninth", "tenth");

        final PCollection<String> pCollection1 = p.apply(Create.of(input1));
        final PCollection<String> pCollection2 = p.apply(Create.of(input2));

        final PCollectionList<String> collectionList = PCollectionList.of(pCollection1).and(pCollection2);
        final PCollection<String> mergedData = collectionList.apply(Flatten.<String>pCollections());


        mergedData.apply(TextIO.write().to("/Users/anujmehra/git/apache-beam/src/main/resources/UnionTwoCollections/")
                .withSuffix(".txt")
                .withDelimiter("|".toCharArray()));

        p.run().waitUntilFinish();
    }
}
