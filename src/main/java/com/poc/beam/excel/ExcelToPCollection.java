package com.poc.beam.excel;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.DoFn;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ExcelToPCollection {

    public static class ReadExcelFn extends DoFn<FileIO.ReadableFile, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            try (InputStream is = c.element().open();
                 Workbook workbook = new XSSFWorkbook(is)) {

                Sheet sheet = workbook.getSheetAt(0);
                for (Row row : sheet) {
                    List<String> rowData = new ArrayList<>();
                    for (Cell cell : row) {
                        cell.setCellType(CellType.STRING);
                        rowData.add(cell.getStringCellValue());
                    }
                    c.output(String.join(",", rowData));  // You can also emit a POJO instead
                }
            }
        }
    }

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<String> lines = p
                .apply("Match Excel File", FileIO.match().filepattern("gs://your-bucket/path/to/file.xlsx"))
                .apply("Read File", FileIO.readMatches())
                .apply("Parse Excel", ParDo.of(new ReadExcelFn()));

        lines.apply("Print Rows", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));

        p.run().waitUntilFinish();
    }
}
