/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.http.sink.batch;

import com.google.gson.stream.JsonWriter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringWriter;

/**
 * Tests for {@link MessageBuffer}
 */
public class MessageBufferTest {
  private static final HTTPSinkConfig VALID_CONFIG = new HTTPSinkConfig(
          "test",
          "http://localhost",
          "GET",
          1,
          ":",
          "JSON",
          "body",
          "",
          "UTF8",
          true,
          true,
          1,
          1,
          1,
          true,
          "false",
          "none",
          "results",
          true);
  MessageBuffer messageBuffer;
  Schema dummySchema = Schema.recordOf("dummy",
          Schema.Field.of("id", Schema.of(Schema.Type.INT)),
          Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("country", Schema.of(Schema.Type.STRING)));
  StructuredRecord[] dummyRecords;
  String[] dummyRecordsJsonString;
  StringWriter stringWriter;
  JsonWriter jsonWriter;

  @Before
  public void setUp() throws Exception {
    stringWriter = new StringWriter();
    jsonWriter = new JsonWriter(stringWriter);
    dummyRecords = new StructuredRecord[]{
            StructuredRecord.builder(dummySchema).set("id", 1).set("name", "John").set("country", "USA").build(),
            StructuredRecord.builder(dummySchema).set("id", 2).set("name", "Jane").set("country", "Canada").build(),
            StructuredRecord.builder(dummySchema).set("id", 3).set("name", "Jack").set("country", "USA").build(),
            StructuredRecord.builder(dummySchema).set("id", 4).set("name", "Jill").set("country", "Canada").build(),
            StructuredRecord.builder(dummySchema).set("id", 5).set("name", "Joe").set("country", "USA").build(),
    };
    dummyRecordsJsonString = new String[]{
            "{\"id\":1,\"name\":\"John\",\"country\":\"USA\"}",
            "{\"id\":2,\"name\":\"Jane\",\"country\":\"Canada\"}",
            "{\"id\":3,\"name\":\"Jack\",\"country\":\"USA\"}",
            "{\"id\":4,\"name\":\"Jill\",\"country\":\"Canada\"}",
            "{\"id\":5,\"name\":\"Joe\",\"country\":\"USA\"}"
    };
  }

  @Test
  public void testAdding5RecordsWithBatchSize1() throws Exception {
    HTTPSinkConfig httpSinkConfigWithBatchSize1 = HTTPSinkConfig.newBuilder(VALID_CONFIG).setBatchSize(1).build();
    messageBuffer = new MessageBuffer(
            httpSinkConfigWithBatchSize1.getMessageFormat(), httpSinkConfigWithBatchSize1.getJsonBatchKey(),
            httpSinkConfigWithBatchSize1.shouldWriteJsonAsArray(),
            httpSinkConfigWithBatchSize1.getDelimiterForMessages(),
            httpSinkConfigWithBatchSize1.getCharset(), httpSinkConfigWithBatchSize1.getBody(), dummySchema
    );
    for (StructuredRecord record : dummyRecords) {
      messageBuffer.add(record);
    }
    Assert.assertEquals(dummyRecords.length, messageBuffer.size());
  }

  @Test
  public void testAdding5RecordsWithBatchSize3() throws Exception {
    HTTPSinkConfig httpSinkConfigWithBatchSize3 = HTTPSinkConfig.newBuilder(VALID_CONFIG).setBatchSize(3).build();
    messageBuffer = new MessageBuffer(
            httpSinkConfigWithBatchSize3.getMessageFormat(), httpSinkConfigWithBatchSize3.getJsonBatchKey(),
            httpSinkConfigWithBatchSize3.shouldWriteJsonAsArray(),
            httpSinkConfigWithBatchSize3.getDelimiterForMessages(),
            httpSinkConfigWithBatchSize3.getCharset(), httpSinkConfigWithBatchSize3.getBody(), dummySchema
    );
    for (StructuredRecord record : dummyRecords) {
      messageBuffer.add(record);
    }
    Assert.assertEquals(dummyRecords.length, messageBuffer.size());
  }

  @Test
  public void testClear() throws Exception {
    HTTPSinkConfig httpSinkConfigWithBatchSize1 = HTTPSinkConfig.newBuilder(VALID_CONFIG).setBatchSize(1).build();
    messageBuffer = new MessageBuffer(
            httpSinkConfigWithBatchSize1.getMessageFormat(), httpSinkConfigWithBatchSize1.getJsonBatchKey(),
            httpSinkConfigWithBatchSize1.shouldWriteJsonAsArray(),
            httpSinkConfigWithBatchSize1.getDelimiterForMessages(),
            httpSinkConfigWithBatchSize1.getCharset(), httpSinkConfigWithBatchSize1.getBody(), dummySchema
    );
    for (StructuredRecord record : dummyRecords) {
      messageBuffer.add(record);
    }
    Assert.assertEquals(dummyRecords.length, messageBuffer.size());
    messageBuffer.clear();
    Assert.assertEquals(0, messageBuffer.size());
  }

  @Test
  public void testIsEmpty() throws Exception {
    HTTPSinkConfig httpSinkConfigWithBatchSize1 = HTTPSinkConfig.newBuilder(VALID_CONFIG).setBatchSize(1).build();
    messageBuffer = new MessageBuffer(
            httpSinkConfigWithBatchSize1.getMessageFormat(), httpSinkConfigWithBatchSize1.getJsonBatchKey(),
            httpSinkConfigWithBatchSize1.shouldWriteJsonAsArray(),
            httpSinkConfigWithBatchSize1.getDelimiterForMessages(),
            httpSinkConfigWithBatchSize1.getCharset(), httpSinkConfigWithBatchSize1.getBody(), dummySchema
    );
    Assert.assertTrue(messageBuffer.isEmpty());
  }

  @Test
  public void testIsEmptyAfterClear() throws Exception {
    HTTPSinkConfig httpSinkConfigWithBatchSize1 = HTTPSinkConfig.newBuilder(VALID_CONFIG).setBatchSize(1).build();
    messageBuffer = new MessageBuffer(
            httpSinkConfigWithBatchSize1.getMessageFormat(), httpSinkConfigWithBatchSize1.getJsonBatchKey(),
            httpSinkConfigWithBatchSize1.shouldWriteJsonAsArray(),
            httpSinkConfigWithBatchSize1.getDelimiterForMessages(),
            httpSinkConfigWithBatchSize1.getCharset(), httpSinkConfigWithBatchSize1.getBody(), dummySchema
    );
    for (StructuredRecord record : dummyRecords) {
      messageBuffer.add(record);
    }
    Assert.assertEquals(dummyRecords.length, messageBuffer.size());
    messageBuffer.clear();
    Assert.assertTrue(messageBuffer.isEmpty());
  }

  @Test
  public void testGetContentTypeWithJsonFormat() throws Exception {
    HTTPSinkConfig httpSinkConfigWithMessageFormatJson = HTTPSinkConfig.newBuilder(VALID_CONFIG)
            .setMessageFormat("JSON").build();
    messageBuffer = new MessageBuffer(
            httpSinkConfigWithMessageFormatJson.getMessageFormat(),
            httpSinkConfigWithMessageFormatJson.getJsonBatchKey(),
            httpSinkConfigWithMessageFormatJson.shouldWriteJsonAsArray(),
            httpSinkConfigWithMessageFormatJson.getDelimiterForMessages(),
            httpSinkConfigWithMessageFormatJson.getCharset(), httpSinkConfigWithMessageFormatJson.getBody(), dummySchema
    );
    Assert.assertEquals("application/json", messageBuffer.getContentType());
  }

  @Test
  public void testGetMessageWithBatchSize1() throws Exception {
    HTTPSinkConfig httpSinkConfigWithBatchSize1 = HTTPSinkConfig.newBuilder(VALID_CONFIG).setBatchSize(1)
            .setMessageFormat("JSON").build();
    messageBuffer = new MessageBuffer(
            httpSinkConfigWithBatchSize1.getMessageFormat(), httpSinkConfigWithBatchSize1.getJsonBatchKey(),
            httpSinkConfigWithBatchSize1.shouldWriteJsonAsArray(),
            httpSinkConfigWithBatchSize1.getDelimiterForMessages(),
            httpSinkConfigWithBatchSize1.getCharset(), httpSinkConfigWithBatchSize1.getBody(), dummySchema
    );

    messageBuffer.add(dummyRecords[0]);

    Assert.assertEquals(dummyRecordsJsonString[0], messageBuffer.getMessage());
  }

  @Test
  public void testGetMessageWithBatchSize3AndJsonArrayTrueAndWrapperKeyEmptyString() throws Exception {
    HTTPSinkConfig httpSinkConfigWithBatchSize3AndJsonArrayTrueAndWrapperKeyEmptyString =
            HTTPSinkConfig.newBuilder(VALID_CONFIG).setBatchSize(3).setMessageFormat("JSON").setWriteJsonAsArray(true)
                    .setJsonBatchKey("").build();
    messageBuffer = new MessageBuffer(
            httpSinkConfigWithBatchSize3AndJsonArrayTrueAndWrapperKeyEmptyString.getMessageFormat(),
            httpSinkConfigWithBatchSize3AndJsonArrayTrueAndWrapperKeyEmptyString.getJsonBatchKey(),
            httpSinkConfigWithBatchSize3AndJsonArrayTrueAndWrapperKeyEmptyString.shouldWriteJsonAsArray(),
            httpSinkConfigWithBatchSize3AndJsonArrayTrueAndWrapperKeyEmptyString.getDelimiterForMessages(),
            httpSinkConfigWithBatchSize3AndJsonArrayTrueAndWrapperKeyEmptyString.getCharset(),
            httpSinkConfigWithBatchSize3AndJsonArrayTrueAndWrapperKeyEmptyString.getBody(), dummySchema
    );

    int batchSize = 3;
    for (int i = 0; i < batchSize; i++) {
      messageBuffer.add(dummyRecords[i]);
    }

    Assert.assertEquals("[" + dummyRecordsJsonString[0] + "," + dummyRecordsJsonString[1] + "," +
            dummyRecordsJsonString[2] + "]", messageBuffer.getMessage());
  }

  @Test
  public void testGetMessageWithBatchSize2AndJsonArrayTrueAndWrapperKeyData() throws Exception {
    HTTPSinkConfig httpSinkConfigWithBatchSize2AndJsonArrayTrueAndWrapperKeyData =
            HTTPSinkConfig.newBuilder(VALID_CONFIG).setBatchSize(2).setMessageFormat("JSON").setWriteJsonAsArray(true)
                    .setJsonBatchKey("data").build();
    messageBuffer = new MessageBuffer(
            httpSinkConfigWithBatchSize2AndJsonArrayTrueAndWrapperKeyData.getMessageFormat(),
            httpSinkConfigWithBatchSize2AndJsonArrayTrueAndWrapperKeyData.getJsonBatchKey(),
            httpSinkConfigWithBatchSize2AndJsonArrayTrueAndWrapperKeyData.shouldWriteJsonAsArray(),
            httpSinkConfigWithBatchSize2AndJsonArrayTrueAndWrapperKeyData.getDelimiterForMessages(),
            httpSinkConfigWithBatchSize2AndJsonArrayTrueAndWrapperKeyData.getCharset(),
            httpSinkConfigWithBatchSize2AndJsonArrayTrueAndWrapperKeyData.getBody(), dummySchema
    );

    int batchSize = 2;
    for (int i = 0; i < batchSize; i++) {
      messageBuffer.add(dummyRecords[i]);
    }

    jsonWriter.beginObject();
    jsonWriter.name("data");
    jsonWriter.beginArray();
    for (int i = 0; i < batchSize; i++) {
      jsonWriter.jsonValue(dummyRecordsJsonString[i]);
    }
    jsonWriter.endArray();
    jsonWriter.endObject();

    Assert.assertEquals(stringWriter.toString(), messageBuffer.getMessage());

  }

  @Test
  public void testGetMessageWithBatchSize4AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter()
          throws Exception {
    HTTPSinkConfig httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter =
            HTTPSinkConfig.newBuilder(VALID_CONFIG).setBatchSize(4).setMessageFormat("JSON")
                    .setWriteJsonAsArray(false).setJsonBatchKey("").setDelimiterForMessages("|").build();
    messageBuffer =
            new MessageBuffer(
                    httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter
                            .getMessageFormat(),
                    httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter
                            .getJsonBatchKey(),
                    httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter
                            .shouldWriteJsonAsArray(),
                    httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter
                            .getDelimiterForMessages(),
                    httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter
                            .getCharset(),
                    httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter.getBody(),
                    dummySchema
            );

    int batchSize = 4;
    for (int i = 0; i < batchSize; i++) {
      messageBuffer.add(dummyRecords[i]);
    }

    Assert.assertEquals(dummyRecordsJsonString[0] + "|" + dummyRecordsJsonString[1] + "|" +
            dummyRecordsJsonString[2] + "|" + dummyRecordsJsonString[3], messageBuffer.getMessage());
  }

  @Test
  public void testGetMessageWithBatchSize4AndJsonArrayFalseAndWrapperKeyDataAndCustomDelimiter()
          throws Exception {
    HTTPSinkConfig httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyDataAndCustomDelimiter =
            HTTPSinkConfig.newBuilder(VALID_CONFIG).setBatchSize(4).setMessageFormat("JSON")
                    .setWriteJsonAsArray(false).setJsonBatchKey("data").setDelimiterForMessages("|").build();
    messageBuffer = new MessageBuffer(
            httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyDataAndCustomDelimiter.getMessageFormat(),
            httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyDataAndCustomDelimiter.getJsonBatchKey(),
            httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyDataAndCustomDelimiter.shouldWriteJsonAsArray(),
            httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyDataAndCustomDelimiter.getDelimiterForMessages(),
            httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyDataAndCustomDelimiter.getCharset(),
            httpSinkConfigWithBatchSize4AndJsonArrayFalseAndWrapperKeyDataAndCustomDelimiter.getBody(), dummySchema
    );

    int batchSize = 4;
    for (int i = 0; i < batchSize; i++) {
      messageBuffer.add(dummyRecords[i]);
    }

    Assert.assertEquals(dummyRecordsJsonString[0] + "|" + dummyRecordsJsonString[1] + "|" +
            dummyRecordsJsonString[2] + "|" + dummyRecordsJsonString[3], messageBuffer.getMessage());
  }

  @Test
  public void testGetMessageWithBatchSize5AndJsonArrayTrueAndWrapperKeyItems() throws Exception {
    HTTPSinkConfig httpSinkConfigWithBatchSize5AndJsonArrayTrueAndWrapperKeyItems =
            HTTPSinkConfig.newBuilder(VALID_CONFIG).setBatchSize(5).setMessageFormat("JSON").setWriteJsonAsArray(true)
                    .setJsonBatchKey("items").build();
    messageBuffer = new MessageBuffer(
            httpSinkConfigWithBatchSize5AndJsonArrayTrueAndWrapperKeyItems.getMessageFormat(),
            httpSinkConfigWithBatchSize5AndJsonArrayTrueAndWrapperKeyItems.getJsonBatchKey(),
            httpSinkConfigWithBatchSize5AndJsonArrayTrueAndWrapperKeyItems.shouldWriteJsonAsArray(),
            httpSinkConfigWithBatchSize5AndJsonArrayTrueAndWrapperKeyItems.getDelimiterForMessages(),
            httpSinkConfigWithBatchSize5AndJsonArrayTrueAndWrapperKeyItems.getCharset(),
            httpSinkConfigWithBatchSize5AndJsonArrayTrueAndWrapperKeyItems.getBody(), dummySchema
    );

    int batchSize = 5;
    for (int i = 0; i < batchSize; i++) {
      messageBuffer.add(dummyRecords[i]);
    }

    jsonWriter.beginObject();
    jsonWriter.name("items");
    jsonWriter.beginArray();
    for (String jsonRecord : dummyRecordsJsonString) {
      jsonWriter.jsonValue(jsonRecord);
    }
    jsonWriter.endArray();
    jsonWriter.endObject();

    Assert.assertEquals(stringWriter.toString(), messageBuffer.getMessage());
  }

  @Test
  public void testGetMessageWithBatchSize1AndJsonArrayTrueAndWrapperKeyData() throws Exception {
    HTTPSinkConfig httpSinkConfigWithBatchSize1AndJsonArrayTrueAndWrapperKeyData =
            HTTPSinkConfig.newBuilder(VALID_CONFIG).setBatchSize(1).setMessageFormat("JSON").setWriteJsonAsArray(true)
                    .setJsonBatchKey("data").build();
    messageBuffer = new MessageBuffer(
            httpSinkConfigWithBatchSize1AndJsonArrayTrueAndWrapperKeyData.getMessageFormat(),
            httpSinkConfigWithBatchSize1AndJsonArrayTrueAndWrapperKeyData.getJsonBatchKey(),
            httpSinkConfigWithBatchSize1AndJsonArrayTrueAndWrapperKeyData.shouldWriteJsonAsArray(),
            httpSinkConfigWithBatchSize1AndJsonArrayTrueAndWrapperKeyData.getDelimiterForMessages(),
            httpSinkConfigWithBatchSize1AndJsonArrayTrueAndWrapperKeyData.getCharset(),
            httpSinkConfigWithBatchSize1AndJsonArrayTrueAndWrapperKeyData.getBody(), dummySchema
    );

    messageBuffer.add(dummyRecords[0]);

    jsonWriter.beginObject();
    jsonWriter.name("data");
    jsonWriter.beginArray();
    jsonWriter.jsonValue(dummyRecordsJsonString[0]);
    jsonWriter.endArray();
    jsonWriter.endObject();

    Assert.assertEquals(stringWriter.toString(), messageBuffer.getMessage());

  }

  @Test
  public void testGetMessageWithBatchSize2AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter()
          throws Exception {
    HTTPSinkConfig httpSinkConfigWithBatchSize2AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter =
            HTTPSinkConfig.newBuilder(VALID_CONFIG).setBatchSize(2).setMessageFormat("JSON")
                    .setWriteJsonAsArray(false).setJsonBatchKey("").setDelimiterForMessages(",").build();
    messageBuffer =
            new MessageBuffer(
                httpSinkConfigWithBatchSize2AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter
                        .getMessageFormat(),
                    httpSinkConfigWithBatchSize2AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter
                            .getJsonBatchKey(),
                    httpSinkConfigWithBatchSize2AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter
                            .shouldWriteJsonAsArray(),
                    httpSinkConfigWithBatchSize2AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter
                            .getDelimiterForMessages(),
                    httpSinkConfigWithBatchSize2AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter
                            .getCharset(),
                    httpSinkConfigWithBatchSize2AndJsonArrayFalseAndWrapperKeyEmptyStringAndCustomDelimiter
                            .getBody(), dummySchema
            );

    int batchSize = 2;
    for (int i = 0; i < batchSize; i++) {
      messageBuffer.add(dummyRecords[i]);
    }

    Assert.assertEquals(dummyRecordsJsonString[0] + "," + dummyRecordsJsonString[1],
            messageBuffer.getMessage());
  }
}
