package com.harland.example.streaming.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface JsonPipelineOptions extends PipelineOptions {

  @Description("PubSub Topic Name")
  @Default.String("projects/my-project/topics/my-topic")
  String getTopic();

  void setTopic(String topic);

  @Description("BigQuery Table Name")
  @Default.String("my-project:my_dataset.my_table")
  String getBqTableName();

  void setBqTableName(String tableName);
}
