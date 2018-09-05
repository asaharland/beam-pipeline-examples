package com.harland.example.streaming;

import com.google.api.services.bigquery.model.TableRow;
import com.harland.example.common.bigquery.Schema;
import com.harland.example.common.model.TransferRecord;
import com.harland.example.common.options.AwsOptionsParser;
import com.harland.example.common.options.BigQueryImportOptions;
import com.harland.example.common.transform.ConvertToTransferRecordFn;
import com.harland.example.common.utils.JsonSchemaReader;
import com.harland.example.common.utils.MathUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import java.io.IOException;

public class StreamingFilePipeline2 {

  private static final String SCHEMA_FILE = "schema.json";

  private static final int WINDOW_SIZE_SECONDS = 30;

  public static void main(String... args) throws IOException {
    BigQueryImportOptions options =
        PipelineOptionsFactory.fromArgs(args).as(BigQueryImportOptions.class);

    // Configure AWS specific options
    AwsOptionsParser.formatOptions(options);

    runPipeline(options);
  }

  private static void runPipeline(BigQueryImportOptions options) throws IOException {
    Pipeline p = Pipeline.create(options);

    Schema schema = new Schema(JsonSchemaReader.readSchemaFile(SCHEMA_FILE));
    String bqColUser = schema.getColumnName(0);
    String bqColAmount = schema.getColumnName(1);

    p.apply(
            "ReadFromStorage",
            TextIO.read()
                .from(options.getBucketUrl() + "/*")
                .watchForNewFiles(Duration.ZERO, Watch.Growth.never()))

        // Apply a windowing function for our unbound source
        .apply(
            String.format("WindowInto%dSecondWindows", WINDOW_SIZE_SECONDS),
            Window.into(FixedWindows.of(Duration.standardSeconds(WINDOW_SIZE_SECONDS))))

        // Convert each CSV row to a transfer record object
        .apply("ConvertToTransferRecord", ParDo.of(new ConvertToTransferRecordFn()))

        // Map our elements into KV pairs by user.
        .apply(
            "CreateKVPairs",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                .via((TransferRecord record) -> KV.of(record.getUser(), record.getAmount())))

        // Sum our KV pairs for each user.
        .apply("SumAmountsPerUser", Sum.doublesPerKey())

        // Write the result to BigQuery.
        .apply(
            "WriteToBigQuery",
            BigQueryIO.<KV<String, Double>>write()
                .to(options.getBqTableName())
                .withSchema(schema.getTableSchema())
                .withFormatFunction(
                    (KV<String, Double> record) ->
                        new TableRow()
                            .set(bqColUser, record.getKey())
                            .set(bqColAmount, MathUtils.roundToTwoDecimals(record.getValue())))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    p.run();
  }
}
