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

package com.pelion.connect.dm.schemas;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class NotificationData {

  public static final String NAME = "com.pelion.kafka.connect.NotificationData";

  public static final String EP_FIELD = "ep";
  public static final String PATH_FIELD = "path";
  public static final String CT_FIELD = "ct";
  public static final String PAYLOAD_B64_FIELD = "payload_b64";
  public static final String MAX_AGE_FIELD = "max_age";
  public static final String UID_FIELD = "uid";
  public static final String TIMESTAMP_FIELD = "timestamp";
  public static final String ORIGINAL_EP_FIELD = "original_ep";
  public static final String PAYLOAD_FIELD = "payload";

  public static final Schema SCHEMA = SchemaBuilder.struct().name(NAME)
      .version(1)
      .field(EP_FIELD, Schema.STRING_SCHEMA)
      .field(PATH_FIELD, Schema.STRING_SCHEMA)
      .field(CT_FIELD, Schema.STRING_SCHEMA)
      .field(PAYLOAD_B64_FIELD, Schema.STRING_SCHEMA)
      .field(PAYLOAD_FIELD, PayloadData.SCHEMA)
      .field(MAX_AGE_FIELD, Schema.INT32_SCHEMA)
      .field(UID_FIELD, Schema.STRING_SCHEMA)
      .field(TIMESTAMP_FIELD, Schema.INT64_SCHEMA)
      .field(ORIGINAL_EP_FIELD, Schema.STRING_SCHEMA)
      .build();

  public static class PayloadData {
    public static final String NAME = "com.pelion.kafka.connect.PayloadData";

    public static final String S_FIELD = "s";
    public static final String L_FIELD = "l";
    public static final String D_FIELD = "d";
    public static final String B_FIELD = "b";

    public static final Schema SCHEMA = SchemaBuilder.struct().name(NAME)
        .version(1)
        .field(S_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
        .field(L_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
        .field(D_FIELD, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(B_FIELD, Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();
  }
}
