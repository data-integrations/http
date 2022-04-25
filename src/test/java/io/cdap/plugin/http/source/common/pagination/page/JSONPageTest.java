/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.http.source.batch.HttpBatchSourceConfig;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;

public class JSONPageTest {

  // The input schema
  private static final Schema INPUT_SCHEMA = Schema.recordOf("input",
          Schema.Field.of("firstName", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("lastName", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("mail", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("_id", Schema.of(Schema.Type.STRING))
  );

  private static final String JSON = "\n" +
          "{\n" +
          "  \"_id\": \"the_id_value\",\n" +
          "  \"mail\": \"toto.tata@tutu.com\",\n" +
          "  \"firstName\": \"toto\",\n" +
          "  \"lastName\": \"tata\"\n" +
          "}";

  private static final String JSON_WITH_EMPTY = "\n" +
          "{\n" +
          "  \"_id\": \"the_id_value\",\n" +
          "  \"mail\": \"toto.tata@tutu.com\"\n" +
          "}";

  static class BaseTestConfig extends HttpBatchSourceConfig {
    BaseTestConfig(String referenceName) {
      super(referenceName);
      this.schema = INPUT_SCHEMA.toString();
      this.url = "";
      this.httpMethod = "GET";
      this.oauth2Enabled = "false";
      this.httpErrorsHandling = "2..:Success,.*:Fail";
      this.retryPolicy = "linear";
      this.maxRetryDuration = 10L;
      this.linearRetryInterval = 1L;
      this.waitTimeBetweenPages = 0L;
      this.connectTimeout = 60;
      this.readTimeout = 120;
      this.format = "json";
      this.keystoreType = "Java KeyStore (JKS)";
      this.trustStoreType = "Java KeyStore (JKS)";
      this.transportProtocols = "TLSv1.2";
    }
  }

  @Test
  public void testJSONPageNominal() {
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(httpResponse.getBody()).thenReturn(JSON);
    BaseTestConfig config = new BaseTestConfig("testJsonPageNominal");
    JsonPage jsonPage = new JsonPage(config, httpResponse);
    PageEntry entry = jsonPage.next();
    StructuredRecord outputRecord = entry.getRecord();
    StructuredRecord expectedRecord = StructuredRecord.builder(INPUT_SCHEMA)
            .set("_id", "the_id_value")
            .set("mail", "toto.tata@tutu.com")
            .set("firstName", "toto")
            .set("lastName", "tata")
            .build();

    Assert.assertEquals(expectedRecord, outputRecord);
  }

  @Test
  public void testJSONPageWithEmpty() {
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(httpResponse.getBody()).thenReturn(JSON_WITH_EMPTY);
    BaseTestConfig config = new BaseTestConfig("testJsonPageWithEmpty");
    JsonPage jsonPage = new JsonPage(config, httpResponse);
    PageEntry entry = jsonPage.next();
    StructuredRecord outputRecord = entry.getRecord();
    StructuredRecord expectedRecord = StructuredRecord.builder(INPUT_SCHEMA)
            .set("_id", "the_id_value")
            .set("mail", "toto.tata@tutu.com")
            .build();

    Assert.assertEquals(expectedRecord, outputRecord);
  }

}
