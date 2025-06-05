package com.poc.beam;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.GcsOptions;

import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Formatter;

public class FileMD5Hash {

    public static void main(String[] args) throws Exception {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        FileSystems.setDefaultPipelineOptions(options); // Required for FileSystems API

        String filePattern = "path/to/your/file.txt"; // works with gs://, file://, etc.
        MatchResult matchResult = FileSystems.match(filePattern);
        if (matchResult.metadata().isEmpty()) {
            throw new RuntimeException("No file found at: " + filePattern);
        }

        ResourceId resourceId = matchResult.metadata().get(0).resourceId();

        try (InputStream inputStream = Channels.newInputStream(FileSystems.open(resourceId))) {
            String md5Hash = computeMD5Hash(inputStream);
            System.out.println("MD5 hash: " + md5Hash);
        }
    }

    private static String computeMD5Hash(InputStream inputStream) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] buffer = new byte[8192];
        int bytesRead;

        while ((bytesRead = inputStream.read(buffer)) != -1) {
            md.update(buffer, 0, bytesRead);
        }

        // Convert byte array to hex string
        try (Formatter formatter = new Formatter()) {
            for (byte b : md.digest()) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        }
    }
}





