package com.harland.example.streaming;

import com.google.api.services.bigquery.model.TableRow;
import com.harland.example.streaming.options.JsonPipelineOptions;
import com.harland.example.utils.SchemaReader;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.io.FileNotFoundException;
import java.util.Map;

public class StreamingJsonPipeline {

  public static void main(String... args) throws FileNotFoundException {
    JsonPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(JsonPipelineOptions.class);
    runPipeline(options);
  }

  private static void runPipeline(JsonPipelineOptions options) throws FileNotFoundException {
    Pipeline p = Pipeline.create(options);

    PCollection<String> messages =
        p.apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()));
    messages.apply(
        "WriteToBigQuery",
        BigQueryIO.<String>write()
            .to(options.getBqTableName())
            .withSchema(new SchemaReader().getTableSchema())
            .withFormatFunction(
                (String message) -> {
                  TableRow tableRow = new TableRow();
                  JsonObject jsonObject = new JsonParser().parse(message).getAsJsonObject();
                  for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                    tableRow.set(entry.getKey(), entry.getValue().getAsString());
                  }
                  return tableRow;
                })
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    p.run();
  }
}
