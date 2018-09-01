package com.harland.example.batch.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Map;

public class Schema {

  private TableSchema tableSchema;

  public Schema(JsonObject schema) {
    parseJson(schema);
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public String getColumnName(int columnPosition) {
    return tableSchema.getFields().get(columnPosition).getName();
  }

  private void parseJson(JsonObject jsonSchema) {
    ImmutableList.Builder<TableFieldSchema> immutableListBuilder = ImmutableList.builder();
    for (Map.Entry<String, JsonElement> entry : jsonSchema.entrySet()) {
      immutableListBuilder.add(
          new TableFieldSchema().setName(entry.getKey()).setType(entry.getValue().getAsString()));
    }
    tableSchema = new TableSchema().setFields(immutableListBuilder.build());
  }
}
