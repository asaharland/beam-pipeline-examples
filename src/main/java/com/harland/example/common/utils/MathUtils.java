package com.harland.example.common.utils;

public class MathUtils {

  public static Double roundToTwoDecimals(Double value) {
    return Math.round(value * 100.0) / 100.0;
  }
}
