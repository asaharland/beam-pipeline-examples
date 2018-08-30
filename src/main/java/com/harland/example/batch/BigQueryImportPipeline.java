package com.harland.example.batch;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.harland.example.batch.options.BigQueryImportOptions;
import com.harland.example.utils.SchemaReader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.IOException;

/**
 * Pipeline for importing CSV data from Google Cloud Storage or AWS S3 and writing it to Google
 * BigQuery.
 */
public class BigQueryImportPipeline {

  public static void main(String... args) throws IOException {
    BigQueryImportOptions options =
        PipelineOptionsFactory.fromArgs(args).as(BigQueryImportOptions.class);
    formatOptions(options);
    runPipeline(options);
  }

  public static void runPipeline(BigQueryImportOptions options) throws IOException {
    Pipeline p = Pipeline.create(options);

    SchemaReader schemaReader = new SchemaReader();
    String[] headerRow = schemaReader.getHeaderRow();
    TableSchema schema = schemaReader.getTableSchema();

    p.apply("ReadFromStorage", TextIO.read().from(options.getBucketUrl() + "/*"))
        .apply("ConvertToTableRow", ParDo.of(new RemoveHeaderRowFn(headerRow)))
        .apply(
            "WriteToBigQuery",
            BigQueryIO.writeTableRows()
                .to(options.getBqTableName())
                .withSchema(schema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    p.run();
  }

  private static class RemoveHeaderRowFn extends DoFn<String, TableRow> {
    private String[] headerRow;

    public RemoveHeaderRowFn(String[] headerRow) {
      this.headerRow = headerRow;
    }

    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<TableRow> receiver) {
      String[] fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
      if (!fields[0].equals(headerRow[0])) {
        TableRow tableRow = new TableRow();
        for (int i = 0; i < fields.length; i++) {
          tableRow.set(headerRow[i], fields[i]);
        }
        receiver.output(tableRow);
      }
    }
  }

  private static void formatOptions(BigQueryImportOptions options) {
    if (options.getBucketUrl().startsWith("s3")) {
      options.setAwsCredentialsProvider(
          new AWSStaticCredentialsProvider(
              new BasicAWSCredentials(options.getAwsAccessKey(), options.getAwsSecretKey())));
    }

    if (options.getAwsRegion() == null) {
      options.setAwsRegion("eu-west-1");
    }
  }

}
