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

public class AsyncIDResponseData {

  public static final String NAME = "com.pelion.kafka.connect.AsyncIDResponseData";

  public static final String ID_FIELD = "id";
  public static final String STATUS_FIELD = "status";
  public static final String ERROR_FIELD = "error";
  public static final String PAYLOAD_FIELD = "payload";
  public static final String CT_FIELD = "ct";
  public static final String MAX_AGE_FIELD = "max_age";

  public static final Schema SCHEMA = SchemaBuilder.struct().name(NAME)
      .version(1)
      .field(ID_FIELD, Schema.STRING_SCHEMA)
      .field(STATUS_FIELD, Schema.INT32_SCHEMA)
      .field(ERROR_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
      .field(PAYLOAD_FIELD, Schema.STRING_SCHEMA)
      .field(CT_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
      .field(MAX_AGE_FIELD, Schema.OPTIONAL_INT32_SCHEMA)
      .build();
}
