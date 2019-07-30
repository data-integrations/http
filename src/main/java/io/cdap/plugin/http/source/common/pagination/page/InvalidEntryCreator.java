/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.http.source.common.pagination.page;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.InvalidEntry;

/**
 * Creates invalid entries.
 */
public class InvalidEntryCreator {
  private static final String ERROR_SCHEMA_BODY_PROPERTY = "body";
  private static final Schema STRING_ERROR_SCHEMA = Schema.recordOf("stringError",
                                                                    Schema.Field.of(ERROR_SCHEMA_BODY_PROPERTY,
                                                                             Schema.of(Schema.Type.STRING)));
  private static final Schema BYTES_ERROR_SCHEMA = Schema.recordOf("bytesError",
                                                                    Schema.Field.of(ERROR_SCHEMA_BODY_PROPERTY,
                                                                                    Schema.of(Schema.Type.BYTES)));

  public static InvalidEntry<StructuredRecord> buildBytesError(byte[] bytes, Throwable ex) {
    String errorText = String.format("Cannot convert bytes to a record. Reason: '%s: %s'",
                                     ex.getClass().getName(), ex.getMessage());

    StructuredRecord.Builder builder = StructuredRecord.builder(BYTES_ERROR_SCHEMA);
    builder.set(ERROR_SCHEMA_BODY_PROPERTY, bytes);
    return new InvalidEntry<>(0, errorText, builder.build());
  }

  public static InvalidEntry<StructuredRecord> buildStringError(String recordBody, Throwable e) {
    String errorText = String.format("Cannot convert line '%s' to a record. Reason: '%s: %s'",
                                     recordBody, e.getClass().getName(), e.getMessage());

    return buildStringError(0, recordBody, errorText);
  }

  public static InvalidEntry<StructuredRecord> buildStringError(int code, String recordBody, String errorText) {
    StructuredRecord.Builder builder = StructuredRecord.builder(STRING_ERROR_SCHEMA);
    builder.set(ERROR_SCHEMA_BODY_PROPERTY, recordBody);
    return new InvalidEntry<>(code, errorText, builder.build());
  }
}
