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

public class EndpointData {

  public static final String NAME = "com.pelion.kafka.connect.EndpointData";

  public static final String EP_FIELD = "ep";
  public static final String ORIGINAL_EP_FIELD = "original_ep";
  public static final String EP_TYPE_FIELD = "ept";
  public static final String QUEUE_FIELD = "q";
  public static final String RD_FIELD = "resources";
  public static final String TIMESTAMP_FIELD = "timestamp";

  public static final Schema SCHEMA = SchemaBuilder.struct().name(NAME)
      .version(1)
      .field(EP_FIELD, Schema.STRING_SCHEMA)
      .field(ORIGINAL_EP_FIELD, Schema.STRING_SCHEMA)
      .field(EP_TYPE_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
      .field(QUEUE_FIELD, Schema.BOOLEAN_SCHEMA)
      .field(RD_FIELD, SchemaBuilder.array(ResourceDataSchema.SCHEMA))
      .field(TIMESTAMP_FIELD, Schema.INT64_SCHEMA)
      .build();

  public static class ResourceDataSchema {
    public static final String NAME = "com.pelion.kafka.connect.ResourceData";

    public static final String PATH_FIELD = "path";
    public static final String IF_FIELD = "if";
    public static final String RT_FIELD = "rt";
    public static final String CT_FIELD = "ct";
    public static final String OBS_FIELD = "obs";

    public static final Schema SCHEMA = SchemaBuilder.struct().name(NAME)
        .version(1)
        .field(PATH_FIELD, Schema.STRING_SCHEMA)
        .field(IF_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
        .field(RT_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
        .field(CT_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
        .field(OBS_FIELD, Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();
  }
}
