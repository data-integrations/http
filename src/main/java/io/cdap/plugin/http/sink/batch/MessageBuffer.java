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

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.http.source.common.http.MessageFormatType;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * MessageBuffer is used to store the structured records in a buffer till the batch size is reached.
 * Once the batch size is reached, the records are converted to the appropriate format and appended to the message.
 * The message is then returned to the HTTPRecordWriter.
 */
public class MessageBuffer {
  private static final String REGEX_HASHED_VAR = "#(\\w+)";
  private final List<StructuredRecord> buffer;
  private final String jsonBatchKey;
  private final Boolean shouldWriteJsonAsArray;
  private final String delimiterForMessages;
  private final String charset;
  private final String customMessageBody;
  private final Function<List<StructuredRecord>, String> messageFormatter;
  private final String contentType;
  private final Schema wrappedMessageSchema;


  /**
   * Constructor for MessageBuffer.
   *
   * @param messageFormat          The format of the message. Can be JSON, FORM or CUSTOM.
   * @param jsonBatchKey           The key to be used for the JSON batch message.
   * @param shouldWriteJsonAsArray Whether the JSON message should be written as an array.
   * @param delimiterForMessages   The delimiter to be used for messages.
   * @param charset                The charset to be used for the message.
   * @param customMessageBody      The custom message body to be used.
   */
  public MessageBuffer(
    MessageFormatType messageFormat, String jsonBatchKey, boolean shouldWriteJsonAsArray,
    String delimiterForMessages, String charset, String customMessageBody, Schema inputSchema
  ) {
    this.jsonBatchKey = jsonBatchKey;
    this.delimiterForMessages = delimiterForMessages;
    this.charset = charset;
    this.shouldWriteJsonAsArray = shouldWriteJsonAsArray;
    this.customMessageBody = customMessageBody;
    this.buffer = new ArrayList<>();
    switch (messageFormat) {
      case JSON:
        messageFormatter = this::formatAsJson;
        contentType = "application/json";
        break;
      case FORM:
        messageFormatter = this::formatAsForm;
        contentType = "application/x-www-form-urlencoded";
        break;
      case CUSTOM:
        messageFormatter = this::formatAsCustom;
        contentType = "text/plain";
        break;
      default:
        throw new IllegalArgumentException("Invalid message format: " + messageFormat);
    }
    // A new StructuredRecord is created with the jsonBatchKey as the
    // field name and the array of records as the value
    Schema bufferRecordArraySchema = Schema.arrayOf(inputSchema);
    wrappedMessageSchema = Schema.recordOf("wrapper",
            Schema.Field.of(jsonBatchKey, bufferRecordArraySchema));
  }

  /**
   * Adds a record to the buffer.
   *
   * @param record The record to be added.
   */
  public void add(StructuredRecord record) {
    buffer.add(record);
  }

  /**
   * Clears the buffer.
   */
  public void clear() {
    buffer.clear();
  }

  /**
   * Returns the size of the buffer.
   */
  public int size() {
    return buffer.size();
  }

  /**
   * Returns whether the buffer is empty.
   */
  public boolean isEmpty() {
    return buffer.isEmpty();
  }

  /**
   * Returns the content type of the message.
   */
  public String getContentType() {
    return contentType;
  }

  /**
   * Converts the buffer to the appropriate format and returns the message.
   */
  public String getMessage() throws IOException {
    return messageFormatter.apply(buffer);
  }

  private String formatAsJson(List<StructuredRecord> buffer) {
    try {
      return formatAsJsonInternal(buffer);
    } catch (IOException e) {
      throw new IllegalStateException("Error formatting JSON message. Reason: " + e.getMessage(), e);
    }
  }

  private String formatAsJsonInternal(List<StructuredRecord> buffer) throws IOException {
    boolean useJsonBatchKey = !Strings.isNullOrEmpty(jsonBatchKey);
    if (!shouldWriteJsonAsArray || !useJsonBatchKey) {
      return getBufferAsJsonList();
    }
    StructuredRecord wrappedMessageRecord = StructuredRecord.builder(wrappedMessageSchema)
            .set(jsonBatchKey, buffer).build();
    return StructuredRecordStringConverter.toJsonString(wrappedMessageRecord);
  }

  private String formatAsForm(List<StructuredRecord> buffer) {
    return buffer.stream()
            .map(this::createFormMessage)
            .collect(Collectors.joining(delimiterForMessages));
  }

  private String formatAsCustom(List<StructuredRecord> buffer) {
    return buffer.stream()
            .map(this::createCustomMessage)
            .collect(Collectors.joining(delimiterForMessages));
  }

  private String getBufferAsJsonList() throws IOException {
    StringBuilder sb = new StringBuilder();
    String delimiter = shouldWriteJsonAsArray ? "," : delimiterForMessages;
    if (shouldWriteJsonAsArray) {
      sb.append("[");
    }
    for (StructuredRecord record : buffer) {
      sb.append(StructuredRecordStringConverter.toJsonString(record));
      sb.append(delimiter);
    }
    if (!buffer.isEmpty()) {
      sb.setLength(sb.length() - delimiter.length());
    }
    if (shouldWriteJsonAsArray) {
      sb.append("]");
    }
    return sb.toString();
  }

  private String createFormMessage(StructuredRecord input) {
    boolean first = true;
    String formMessage = null;
    StringBuilder sb = new StringBuilder("");
    for (Schema.Field field : input.getSchema().getFields()) {
      if (first) {
        first = false;
      } else {
        sb.append("&");
      }
      sb.append(field.getName());
      sb.append("=");
      sb.append((String) input.get(field.getName()));
    }
    try {
      formMessage = URLEncoder.encode(sb.toString(), charset);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("Error encoding Form message. Reason: " + e.getMessage(), e);
    }
    return formMessage;
  }

  private String createCustomMessage(StructuredRecord input) {
    String customMessage = customMessageBody;
    Matcher matcher = Pattern.compile(REGEX_HASHED_VAR).matcher(customMessage);
    HashMap<String, String> findReplaceMap = new HashMap();
    while (matcher.find()) {
      if (input.get(matcher.group(1)) != null) {
        findReplaceMap.put(matcher.group(1), (String) input.get(matcher.group(1)));
      } else {
        throw new IllegalArgumentException(String.format(
                "Field %s doesnt exist in the input schema.", matcher.group(1)));
      }
    }
    Matcher replaceMatcher = Pattern.compile(REGEX_HASHED_VAR).matcher(customMessage);
    while (replaceMatcher.find()) {
      String val = replaceMatcher.group().replace("#", "");
      customMessage = (customMessage.replace(replaceMatcher.group(), findReplaceMap.get(val)));
    }
    return customMessage;
  }

}
