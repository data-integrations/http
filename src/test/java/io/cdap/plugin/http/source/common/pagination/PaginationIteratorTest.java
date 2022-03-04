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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.http.source.batch.HttpBatchSourceConfig;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.http.HttpClient;
import io.cdap.plugin.http.source.common.pagination.page.BasePage;
import io.cdap.plugin.http.source.common.pagination.page.JSONUtil;
import io.cdap.plugin.http.source.common.pagination.page.PageEntry;
import io.cdap.plugin.http.source.common.pagination.page.PageFormat;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

public class PaginationIteratorTest {
  private CloseableHttpResponse responseMock;

  @Test
  public void testNonePagination() throws IOException {
    class TestConfig extends BaseTestConfig {
      TestConfig(String referenceName) {
        super(referenceName);
        this.paginationType = "None";
      }
    }

    String[] responses = {"testResponse1"};
    BaseHttpSourceConfig config = new TestConfig("testNonePagination");
    List<StructuredRecord> results =
      getResultsFromIterator(getPaginationIterator(config, responses));
    assertResults(results, responses, config);
  }

  @Test
  public void testIncrementAnIndex() throws IOException {
    class TestConfig extends BaseTestConfig {
      TestConfig(String referenceName) {
        super(referenceName);
        this.paginationType = "Increment an index";
        this.startIndex = 0L;
        this.indexIncrement = 10L;
        this.maxIndex = 40L;
      }
    }
    String[] responses = {"testResponse1", "testResponse2", "testResponse3", "testResponse4", "testResponse5"};

    BaseHttpSourceConfig config = new TestConfig("testIncrementAnIndex");
    List<StructuredRecord> results = getResultsFromIterator(getPaginationIterator(config, responses));
    assertResults(results, responses, config);
  }

  @Test
  public void testLinkInResponseBody() throws IOException {
    class TestConfig extends BaseTestConfig {
      TestConfig(String referenceName) {
        super(referenceName);
        this.format = "json";
        this.paginationType = "Link in response body";
        this.resultPath = "/";
        this.nextPageFieldPath = "/next";
      }
    }
    String[] responses = {"{\"body\": \"testResponse1\", \"next\":\"p2\"}",
      "{\"body\": \"testResponse2\", \"next\":\"p3\"}",
      "{\"body\": \"testResponse3\"}"};

    BaseHttpSourceConfig config = new TestConfig("testLinkInResponseBody");
    List<StructuredRecord> results = getResultsFromIterator(getPaginationIterator(config, responses));
    assertResults(results, responses, config);
  }

  @Test
  public void testLinkInResponseHeader() throws IOException {
    class TestConfig extends BaseTestConfig {
      TestConfig(String referenceName) {
        super(referenceName);
        this.resultPath = "/items";
        this.paginationType = "Link in response header";
      }
    }
    String[] responses = {"testResponse1", "testResponse2", "testResponse3"};

    BaseHttpSourceConfig config = new TestConfig("testLinkInResponseHeader");
    BaseHttpPaginationIterator paginationIterator = getPaginationIterator(config, responses);

    String value1 = "<https://api.github.com/search/code?q=Salesforce%2Buser%3Adata-integrations&page=2>; " +
      "rel=\"next\", <https://api.github.com/search/code?q=Salesforce%2Buser%3Adata-integrations&page=3>; " +
      "rel=\"last\"";
    String value2 = "<https://api.github.com/search/code?q=Salesforce%2Buser%3Adata-integrations&page=2>; " +
      "rel=\"next\", <https://api.github.com/search/code?q=Salesforce%2Buser%3Adata-integrations&page=3>; " +
      "rel=\"last\"";
    String value3 = "<https://api.github.com/search/code?q=Salesforce%2Buser%3Adata-integrations&page=2>; " +
      "rel=\"prev\", <https://api.github.com/search/code?q=Salesforce%2Buser%3Adata-integrations&page=1>; " +
      "rel=\"first\"";
    Mockito.when(responseMock.getFirstHeader(Mockito.anyString()))
      .thenReturn(new BasicHeader("Link", value1))
      .thenReturn(new BasicHeader("Link", value2))
      .thenReturn(new BasicHeader("Link", value3))
      .thenThrow(new RuntimeException("No mock headers to return"));

    List<StructuredRecord> results = getResultsFromIterator(paginationIterator);
    assertResults(results, responses, config);
  }

  @Test
  public void testTokenPagination() throws IOException {
    class TestConfig extends BaseTestConfig {
      TestConfig(String referenceName) {
        super(referenceName);
        this.format = "json";
        this.resultPath = "/";
        this.paginationType = "Token in response body";
        this.nextPageTokenPath = "/nextPageToken";
        this.nextPageUrlParameter = "pageToken";
      }
    }

    String[] responses = {"{\"body\": \"testResponse1\", \"nextPageToken\":\"p2\"}",
      "{\"body\": \"testResponse2\", \"nextPageToken\":\"p3\"}",
      "{\"body\": \"testResponse3\"}"};

    BaseHttpSourceConfig config = new TestConfig("testTokenPagination");
    List<StructuredRecord> results = getResultsFromIterator(getPaginationIterator(config, responses));
    assertResults(results, responses, config);
  }

  @Test
  public void testPaginationCustom() throws IOException {
    class TestConfig extends BaseTestConfig {
      TestConfig(String referenceName) {
        super(referenceName);
        this.paginationType = "Custom";
        this.format = "json";
        this.resultPath = "/";
        this.customPaginationCode = "import json\n" +
          "\n" +
          "def get_next_page_url(url, page, headers):\n" +
          "    \"\"\"\n" +
          "    Based on previous page data generates next page url, when \"Custom pagination\" is enabled.\n" +
          "\n" +
          "    Args:\n" +
          "        url (string): previous page url\n" +
          "        page (string): a body of previous page\n" +
          "        headers (dict): a dictionary of headers from previous page\n" +
          "\n" +
          "    \"\"\"\n" +
          "    page_json = json.loads(page)\n" +
          "    next_page_num = page_json['nextpage']\n" +
          "    \n" +
          "    # stop the iteration\n" +
          "    if next_page_num == None or next_page_num >= 3:\n" +
          "      return None\n" +
          "      \n" +
          "    return \"https://searchcode.com/api/codesearch_I/?q=curl&p={}\".format(next_page_num)\n";
      }
    }

    String[] responses = {"{\"body\": \"testResponse1\", \"nextpage\": 1}",
      "{\"body\": \"testResponse2\", \"nextpage\": 2}",
      "{\"body\": \"testResponse3\", \"nextpage\": 3}"};

    BaseHttpSourceConfig config = new TestConfig("testTokenPagination");
    BaseHttpPaginationIterator paginationIterator = getPaginationIterator(config, responses);

    Mockito.when(responseMock.getAllHeaders()).thenReturn(new Header[0]);
    List<StructuredRecord> results = getResultsFromIterator(paginationIterator);
    assertResults(results, responses, config);
  }

  @Test(expected = IllegalStateException.class)
  public void testErrorHttpStatus() throws IOException {
    class TestConfig extends BaseTestConfig {
      TestConfig(String referenceName) {
        super(referenceName);
        this.paginationType = "None";
      }
    }

    String[] responses = {"testResponse1"};
    BaseHttpSourceConfig config = new TestConfig("testTokenPagination");
    getResultsFromIterator(getPaginationIterator(config, responses, 400));
  }

  private BaseHttpPaginationIterator getPaginationIterator(BaseHttpSourceConfig config, String[] responses)
    throws IOException {

    return getPaginationIterator(config, responses, 200);
  }

  private BaseHttpPaginationIterator getPaginationIterator(BaseHttpSourceConfig config, String[] responses,
                                                           int httpCode) throws IOException {
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(httpCode);

    // StringEntity
    responseMock = Mockito.mock(CloseableHttpResponse.class);
    Mockito.when(responseMock.getStatusLine()).thenReturn(statusLine);

    OngoingStubbing<HttpEntity> httpEntityOngoingStubbing = Mockito.when(responseMock.getEntity());
    for (String responseString : responses) {
      httpEntityOngoingStubbing =
        httpEntityOngoingStubbing.thenReturn(new StringEntity(responseString, "UTF-8"));
    }
    httpEntityOngoingStubbing.thenThrow(new RuntimeException("Unexpected http call. No mock responses to return"));

    HttpClient httpClientMock = Mockito.mock(HttpClient.class);
    Mockito.when(httpClientMock.executeHTTP(Mockito.anyString())).thenReturn(responseMock);

    BaseHttpPaginationIterator paginationIterator = Mockito.spy(PaginationIteratorFactory.createInstance(config, null));
    Mockito.when(paginationIterator.getHttpClient()).thenReturn(httpClientMock);

    /*
    HttpErrorHandler httpErrorHandler = new HttpErrorHandler(config);
    Iterator<String> iterator = Arrays.asList(responses).iterator();

    Mockito.doAnswer((x) -> new MockPage(config, httpErrorHandler, new HttpResponse(responseMock),
                                         iterator.next(), !iterator.hasNext()))
      .when(paginationIterator).createPageInstance(Mockito.any(), Mockito.any(), Mockito.any());
    */
    return paginationIterator;
  }

  private static class MockPage extends BasePage {
    private final String value;
    private final boolean returnNullPath;
    private final Schema schema;
    private boolean isReturned = false;

    MockPage(BaseHttpSourceConfig config, String value, boolean returnNullPath) {
      super(null);
      this.value = value;
      this.returnNullPath = returnNullPath;
      this.schema = config.getSchema();
    }

    @Nullable
    @Override
    public String getPrimitiveByPath(String path) {
      if (returnNullPath) {
        return null;
      }
      return "some-value";
    }

    @Override
    public boolean hasNext() {
      return !isReturned;
    }

    @Override
    public PageEntry next() {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      builder.set(schema.getFields().get(0).getName(), value);
      isReturned = true;
      return new PageEntry(builder.build());
    }

    @Override
    public void close() {
    }
  }

  private void assertResults(List<StructuredRecord> results, String[] responses, BaseHttpSourceConfig config) {
    Assert.assertEquals(responses.length, results.size());
    Mockito.verify(responseMock, Mockito.times(responses.length)).getStatusLine();

    int i = 0;
    for (StructuredRecord record : results) {
      String text;
      if (config.getFormat().equals(PageFormat.JSON)) {
        text = JSONUtil.toJsonObject(responses[i++]).get("body").getAsString();
      } else {
        text = responses[i++];
      }
      Assert.assertEquals(text, record.get("body"));
    }
  }

  private List<StructuredRecord> getResultsFromIterator(BaseHttpPaginationIterator paginationIterator) {

    List<StructuredRecord> results = new ArrayList<>();

    if (!paginationIterator.hasNext()) {
      Assert.fail(String.format("Expected results, but returned none."));
    }

    while (paginationIterator.hasNext()) {
      BasePage page = paginationIterator.next();

      while (page.hasNext()) {
        PageEntry pageEntry = page.next();
        results.add(pageEntry.getRecord());
      }
    }
    return results;
  }

  static class BaseTestConfig extends HttpBatchSourceConfig {
    BaseTestConfig(String referenceName) {
      super(referenceName);

      this.schema = "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":" +
        "[{\"name\":\"body\",\"type\":\"string\"}]}";
      this.url = "";
      this.httpMethod = "GET";
      this.oauth2Enabled = "false";
      this.serviceAccountEnabled = "false";
      this.httpErrorsHandling = "2..:Success,.*:Fail";
      this.retryPolicy = "linear";
      this.maxRetryDuration = 10L;
      this.linearRetryInterval = 1L;
      this.waitTimeBetweenPages = 0L;
      this.connectTimeout = 60;
      this.readTimeout = 120;
      this.format = "text";
      this.keystoreType = "Java KeyStore (JKS)";
      this.trustStoreType = "Java KeyStore (JKS)";
      this.transportProtocols = "TLSv1.2";
    }
  }
}
