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

package io.cdap.plugin.http.source.common.pagination;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.Field;
import io.cdap.plugin.http.common.http.HttpResponse;
import io.cdap.plugin.http.common.pagination.page.BasePage;
import io.cdap.plugin.http.common.pagination.page.PageFactory;
import io.cdap.plugin.http.common.pagination.page.PageFormat;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.util.LinkedList;

public class RecordsPerLinesPageTest {

  @Test
  public void testPageIterationMechanism () throws Exception {
    CloseableHttpResponse clResponse = Mockito.mock(CloseableHttpResponse.class);
    HttpResponse response = new HttpResponse(clResponse);
    BaseHttpSourceConfig config = Mockito.mock(BaseHttpSourceConfig.class);
    Mockito.when(config.getFormat()).thenReturn(PageFormat.TEXT);
    LinkedList sampleField = new LinkedList<Field>();
    sampleField.add(Field.of("output", Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf("inputSchema", sampleField);
    Mockito.when(config.getSchema()).thenReturn(schema);
    BasePage page = PageFactory.createInstance(config, response, null, false);
    String testString = "This is a test string.";
    byte[] testBytes = testString.getBytes();
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(testBytes));
    Mockito.when(clResponse.getEntity()).thenReturn(httpEntity);
    Assert.assertEquals(testString, response.getBody());
    if (page.hasNext()) {
      String actualString = page.next().getRecord().get("output");
      Assert.assertTrue(testString.equals(actualString));
    }
  }
}
