/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.plugin.http.common.pagination.page;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.http.source.batch.HttpBatchSourceConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Unit Test for class {@link DelimitedPage}
 */
public class DelimitedPageTest {
  HttpBatchSourceConfig config;

  @Test
  public void testGetStructuredRecordByString() throws IOException {
    Schema schema = Schema.recordOf("inputSchema",
            Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("address", Schema.of(Schema.Type.STRING)));
    config = HttpBatchSourceConfig.builder()
            .setUrl("http://localhost:10000")
            .setFormat("csv")
            .setEnableQuotesValues(true)
            .setSchema(schema.toString())
            .build();
    String id = "1";
    String address = "123 Main St, San Francisco, CA 94105";
    String line = id + ",\"" + address + "\"";
    StructuredRecord structuredRecord;
    try (DelimitedPage delimitedPage = new DelimitedPage(config, null, ",")) {
      structuredRecord = delimitedPage.getStructedRecordByString(line);
    }
    Assert.assertEquals(id, structuredRecord.get("id"));
    Assert.assertEquals(address, structuredRecord.get("address"));
  }

  @Test
  public void testGetStructuredRecordByStringNormal() throws IOException {
    Schema schema = Schema.recordOf("inputSchema",
            Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    config = HttpBatchSourceConfig.builder()
            .setUrl("http://localhost:10000")
            .setFormat("csv")
            .setEnableQuotesValues(true)
            .setSchema(schema.toString())
            .build();
    String id = "1";
    String name = "John";
    String line = id + "," + name;
    StructuredRecord structuredRecord;
    try (DelimitedPage delimitedPage = new DelimitedPage(config, null, ",")) {
      structuredRecord = delimitedPage.getStructedRecordByString(line);
    }
    Assert.assertEquals(id, structuredRecord.get("id"));
    Assert.assertEquals(name, structuredRecord.get("name"));
  }
}
