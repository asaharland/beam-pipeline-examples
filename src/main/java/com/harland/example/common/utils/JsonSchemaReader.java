package com.harland.example.common.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Objects;

public class JsonSchemaReader {

  public static JsonObject readSchemaFile(String schemaFileName) throws FileNotFoundException {
    File schemaFile =
        new File(Objects.requireNonNull(JsonSchemaReader.class.getClassLoader().getResource(schemaFileName)).getFile());
    return new Gson().fromJson(new FileReader(schemaFile), JsonObject.class);
  }
}
