package com.poc.beam.lineage;

import org.apache.avro.Schema;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;

import java.io.InputStream;
import java.nio.channels.Channels;


public class SchemaLoader {

    public static org.apache.avro.Schema getSchema(final String avscSchemaFilePath) throws Exception{

        org.apache.avro.Schema avroSchema = null;
        final MatchResult matchResult = FileSystems.match(avscSchemaFilePath);
        if(matchResult.status() == MatchResult.Status.OK
        && !matchResult.metadata().isEmpty()){
            final ResourceId resourceId = matchResult.metadata().get(0).resourceId();
            try(InputStream inputStream = Channels.newInputStream(FileSystems.open(resourceId))){
                avroSchema = new Schema.Parser().parse(inputStream);
            }catch(Exception e){
                e.printStackTrace();
                throw e;
            }
        }
        return avroSchema;
    }
}
