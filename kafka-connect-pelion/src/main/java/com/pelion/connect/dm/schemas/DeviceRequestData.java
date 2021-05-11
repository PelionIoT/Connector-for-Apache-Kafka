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

public class DeviceRequestData {

  public static final String NAME = "com.pelion.kafka.connect.DeviceRequestData";

  public static final String EP_FIELD = "ep";
  public static final String ASYNC_ID_FIELD = "async_id";
  public static final String RETRY_FIELD = "retry";
  public static final String EXPIRY_SECONDS_FIELD = "expiry_seconds";
  public static final String BODY_FIELD = "body";

  public static final Schema SCHEMA = SchemaBuilder.struct().name(NAME)
      .version(1)
      .field(EP_FIELD, Schema.STRING_SCHEMA)
      .field(ASYNC_ID_FIELD, Schema.STRING_SCHEMA)
      .field(RETRY_FIELD, Schema.OPTIONAL_INT32_SCHEMA)
      .field(EXPIRY_SECONDS_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(BODY_FIELD, BodyData.SCHEMA)
      .build();

  public static class BodyData {
    public static final String NAME = "com.pelion.kafka.connect.BodyData";

    public static final String METHOD_FIELD = "method";
    public static final String URI_FIELD = "uri";
    public static final String ACCEPT_FIELD = "accept";
    public static final String CONTENT_TYPE_FIELD = "content_type";
    public static final String PAYLOAD_B64_FIELD = "payload_b64";

    public static final Schema SCHEMA = SchemaBuilder.struct().name(NAME)
        .version(1)
        .field(METHOD_FIELD, Schema.STRING_SCHEMA)
        .field(URI_FIELD, Schema.STRING_SCHEMA)
        .field(ACCEPT_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
        .field(CONTENT_TYPE_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
        .field(PAYLOAD_B64_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
        .build();
  }
}
