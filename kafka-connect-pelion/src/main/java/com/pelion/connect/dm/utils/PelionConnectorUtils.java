/*
 * Copyright 2021 Pelion Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pelion.connect.dm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Calendar;
import java.util.Properties;

public class PelionConnectorUtils {

  private static final Logger LOG = LoggerFactory.getLogger(PelionConnectorUtils.class);

  private static final String PATH = "/kafka-connect-pelion-version.properties";
  private static String version = "unknown";

  static {
    try (InputStream stream = PelionConnectorUtils.class.getResourceAsStream(PATH)) {
      final Properties props = new Properties();
      props.load(stream);
      version = props.getProperty("version", version).trim();
    } catch (Exception e) {
      LOG.warn("Error while loading version:", e);
    }
  }

  private PelionConnectorUtils() {
  }

  public static String getVersion() {
    return version;
  }

  public static String readFile(String filename) {
    StringBuilder sb = new StringBuilder();

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(PelionConnectorUtils.class.getResourceAsStream("/" + filename)))) {

      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line).append(System.lineSeparator());
      }

      return sb.toString();
    } catch (IOException exception) {
      LOG.info("an exception occurred reading file: {}", exception.getMessage());
    }

    return sb.toString();
  }

  public static String getFutureDateFromNow(int days) {
    Calendar c = Calendar.getInstance();
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    c.add(Calendar.DATE, days);

    return df.format(c.getTime());
  }

  public static String base64Decode(String text) {
    return new String(Base64.getDecoder().decode(text));
  }

  public static void sleep(int ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
