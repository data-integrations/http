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

import io.cdap.plugin.http.source.batch.HttpBatchSourceConfig;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.http.HttpClient;
import io.cdap.plugin.http.source.common.pagination.page.BasePage;
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
import java.util.Arrays;
import java.util.Iterator;
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
    List<String> results =
      getResultsFromIterator(getPaginationIterator(new TestConfig("testNonePagination"), responses));
    assertResults(results, responses);
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
    List<String> results =
      getResultsFromIterator(getPaginationIterator(new TestConfig("testIncrementAnIndex"), responses));
    assertResults(results, responses);
  }

  @Test
  public void testLinkInResponseBody() throws IOException {
    class TestConfig extends BaseTestConfig {
      TestConfig(String referenceName) {
        super(referenceName);
        this.format = "json";
        this.paginationType = "Link in response body";
        this.nextPageFieldPath = "/next";
      }
    }
    String[] responses = {"testResponse1", "testResponse2", "testResponse3"};
    List<String> results =
      getResultsFromIterator(getPaginationIterator(new TestConfig("testLinkInResponseBody"), responses));
    assertResults(results, responses);
  }

  @Test
  public void testLinkInResponseHeader() throws IOException {
    class TestConfig extends BaseTestConfig {
      TestConfig(String referenceName) {
        super(referenceName);
        this.format = "json";
        this.resultPath = "/items";
        this.paginationType = "Link in response header";
      }
    }
    String[] responses = {"testResponse1", "testResponse2", "testResponse3"};
    BaseHttpPaginationIterator paginationIterator = getPaginationIterator(
      new TestConfig("testLinkInResponseHeader"), responses);
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

    List<String> results = getResultsFromIterator(paginationIterator);
    assertResults(results, responses);
  }

  @Test
  public void testTokenPagination() throws IOException {
    class TestConfig extends BaseTestConfig {
      TestConfig(String referenceName) {
        super(referenceName);
        this.paginationType = "Token in response body";
        this.nextPageTokenPath = "/nextPageToken";
        this.nextPageUrlParameter = "pageToken";
      }
    }

    String[] responses = {"testResponse1", "testResponse2", "testResponse3"};
    List<String> results =
      getResultsFromIterator(getPaginationIterator(new TestConfig("testTokenPagination"), responses));
    assertResults(results, responses);
  }

  @Test
  public void testPaginationCustom() throws IOException {
    class TestConfig extends BaseTestConfig {
      TestConfig(String referenceName) {
        super(referenceName);
        this.paginationType = "Custom";
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

    String[] responses = {"{\"nextpage\": 1}", "{\"nextpage\": 2}", "{\"nextpage\": 3}"};
    BaseHttpPaginationIterator paginationIterator =
      getPaginationIterator(new TestConfig("testTokenPagination"), responses);

    Mockito.when(responseMock.getAllHeaders()).thenReturn(new Header[0]);
    List<String> results = getResultsFromIterator(paginationIterator);
    assertResults(results, responses);
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
    getResultsFromIterator(getPaginationIterator(new TestConfig("testTokenPagination"),
                                                 responses, 400));
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

    BaseHttpPaginationIterator paginationIterator = Mockito.spy(PaginationIteratorFactory.createInstance(config));
    Mockito.when(paginationIterator.getHttpClient()).thenReturn(httpClientMock); // TODO: change to doReturn

    Iterator<String> iterator = Arrays.asList(responses).iterator();
    Mockito.doAnswer((x) -> new MockPage(iterator.next(), !iterator.hasNext()))
      .when(paginationIterator).createPageInstance(Mockito.any(), Mockito.anyString(), Mockito.any());

    return paginationIterator;
  }

  private static class MockPage implements BasePage {
    private final String value;
    private final boolean returnNullPath;
    private boolean isReturned = false;

    MockPage(String value, boolean returnNullPath) {
      this.value = value;
      this.returnNullPath = returnNullPath;
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
    public String next() {
      isReturned = true;
      return value;
    }
  }

  private void assertResults(List<String> results, String[] responses) {
    Assert.assertEquals(responses.length, results.size());
    Mockito.verify(responseMock, Mockito.times(responses.length)).getEntity();

    int i = 0;
    for (String pageBody : results) {
      Assert.assertEquals(responses[i++], pageBody);
    }
  }

  private List<String> getResultsFromIterator(BaseHttpPaginationIterator paginationIterator) {
    List<String> results = new ArrayList<>();

    if (!paginationIterator.hasNext()) {
      Assert.fail(String.format("Expected results, but returned none."));
    }

    while (paginationIterator.hasNext()) {
      PageEntry pageEntry = paginationIterator.next();
      results.add(pageEntry.getBody());
      // System.out.println(String.format("%d %s", pageEntry.getHttpCode(), pageEntry.getBody()));
    }
    return results;
  }

  static class BaseTestConfig extends HttpBatchSourceConfig {
    BaseTestConfig(String referenceName) {
      super(referenceName);

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
      this.format = "text";
      this.keystoreType = "Java KeyStore (JKS)";
      this.trustStoreType = "Java KeyStore (JKS)";
      this.transportProtocols = "TLSv1.2";
    }
  }
}
