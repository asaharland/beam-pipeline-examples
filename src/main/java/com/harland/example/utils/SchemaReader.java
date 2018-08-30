package com.harland.example.utils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class SchemaReader {

  private static final String SCHEMA_FILE = "schema.json";

  private static File schemaFile;

  public SchemaReader() {
    schemaFile = new File(getClass().getClassLoader().getResource(SCHEMA_FILE).getFile());
  }

  public String[] getHeaderRow() throws IOException {
    JsonObject jsonObject = readSchemaFile();
    return jsonObject.keySet().toArray(new String[jsonObject.size()]);
  }

  public TableSchema getTableSchema() throws FileNotFoundException {
    ImmutableList.Builder<TableFieldSchema> immutableListBuilder = ImmutableList.builder();
    for (Map.Entry<String, JsonElement> entry : readSchemaFile().entrySet()) {
      immutableListBuilder.add(
          new TableFieldSchema().setName(entry.getKey()).setType(entry.getValue().getAsString()));
    }
    return new TableSchema().setFields(immutableListBuilder.build());
  }

  private JsonObject readSchemaFile() throws FileNotFoundException {
    return new Gson().fromJson(new FileReader(schemaFile), JsonObject.class);
  }
}
