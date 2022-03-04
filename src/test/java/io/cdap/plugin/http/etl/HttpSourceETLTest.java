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
package io.cdap.plugin.http.etl;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public abstract class HttpSourceETLTest extends HydratorTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);
  @Rule
  public TestName testName = new TestName();
  @Rule
  public WireMockRule wireMockRule = new WireMockRule();

  @Test
  public void testIncrementAnIndex() throws Exception {
    Schema schema = Schema.recordOf("etlSchemaBody",
                                    Schema.Field.of("key",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("type",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("isSubtask",
                                                    Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("description",
                                                    Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("projectCategory",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("fixVersions", Schema.arrayOf(
                                      Schema.recordOf("fixVersionsEtlRecord",
                                                      Schema.Field.of("id",
                                                                      Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("name",
                                                                      Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("released",
                                                                      Schema.of(Schema.Type.BOOLEAN))
                                      )
                                    ))

    );
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(BaseHttpSourceConfig.PROPERTY_URL,
           getServerAddress() + "/rest/api/2/search?maxResults=2&startAt={pagination.index}")
           //"https://issues.cask.co/rest/api/2/search?maxResults=2&startAt={pagination.index}")
      .put(BaseHttpSourceConfig.PROPERTY_FORMAT, "json")
      .put(BaseHttpSourceConfig.PROPERTY_RESULT_PATH, "/issues")
      .put(BaseHttpSourceConfig.PROPERTY_FIELDS_MAPPING,
           "type:/fields/issuetype/name,description:/fields/description," +
             "projectCategory:/fields/project/projectCategory/name,isSubtask:/fields/issuetype/subtask," +
             "fixVersions:/fields/fixVersions")
      .put(BaseHttpSourceConfig.PROPERTY_SCHEMA, schema.toString())
      .put(BaseHttpSourceConfig.PROPERTY_PAGINATION_TYPE, "Increment an index")
      .put(BaseHttpSourceConfig.PROPERTY_START_INDEX, "0")
      .put(BaseHttpSourceConfig.PROPERTY_INDEX_INCREMENT, "2")
      .put(BaseHttpSourceConfig.PROPERTY_MAX_INDEX, "6")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/rest/api/2/search?maxResults=2&startAt=0"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testIncrementAnIndex1.txt"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/rest/api/2/search?maxResults=2&startAt=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testIncrementAnIndex2.txt"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/rest/api/2/search?maxResults=2&startAt=4"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testIncrementAnIndex3.txt"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/rest/api/2/search?maxResults=2&startAt=6"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testIncrementAnIndex4.txt"))));

    List<StructuredRecord> records = getPipelineResults(properties, 8);
    Assert.assertEquals(8, records.size());
  }

  @Test
  public void testIncrementAnIndexXml() throws Exception {
    Schema schema = Schema.recordOf("etlSchemaBody",
                                    Schema.Field.of("companyName",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("postalCode",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("country",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("phone",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("fax",
                                                    Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );
    // https://services.odata.org
    // /V3/Northwind/Northwind.svc/Customers?$inlinecount=allpages&$top=5&$skip={pagination.index}
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(BaseHttpSourceConfig.PROPERTY_URL, getServerAddress() + "/V3/Northwind/Northwind.svc" +
        "/Customers?$inlinecount=allpages&$top=5&$skip={pagination.index}")
      .put(BaseHttpSourceConfig.PROPERTY_FORMAT, "xml")
      .put(BaseHttpSourceConfig.PROPERTY_RESULT_PATH, "/feed/entry")
      .put(BaseHttpSourceConfig.PROPERTY_FIELDS_MAPPING,
           "companyName:content/properties/CompanyName,postalCode:content/properties/PostalCode," +
             "country:content/properties/Country,phone:content/properties/Phone,fax:content/properties/Fax/text()")
      .put(BaseHttpSourceConfig.PROPERTY_SCHEMA, schema.toString())
      .put(BaseHttpSourceConfig.PROPERTY_PAGINATION_TYPE, "Increment an index")
      .put(BaseHttpSourceConfig.PROPERTY_START_INDEX, "0")
      .put(BaseHttpSourceConfig.PROPERTY_INDEX_INCREMENT, "20")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/V3/Northwind/Northwind.svc/Customers?$inlinecount=allpages&$top=5&$skip=0"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testIncrementAnIndexXml1.txt"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/V3/Northwind/Northwind.svc/Customers?$inlinecount=allpages&$top=5&$skip=20"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testIncrementAnIndexXml2.txt"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/V3/Northwind/Northwind.svc/Customers?$inlinecount=allpages&$top=5&$skip=40"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testIncrementAnIndexXml3.txt"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/V3/Northwind/Northwind.svc/Customers?$inlinecount=allpages&$top=5&$skip=60"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testIncrementAnIndexXml4.txt"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/V3/Northwind/Northwind.svc/Customers?$inlinecount=allpages&$top=5&$skip=80"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testIncrementAnIndexXml5.txt"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/V3/Northwind/Northwind.svc/Customers?$inlinecount=allpages&$top=5&$skip=100"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testIncrementAnIndexXml6.txt"))));

    List<StructuredRecord> records = getPipelineResults(properties, 25);
    Assert.assertEquals(25, records.size());
  }

  @Test
  public void testLinkInResponseBody() throws Exception {
    Schema schema = Schema.recordOf("etlSchemaBody",
                                    Schema.Field.of("title",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("status",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("type",
                                                    Schema.of(Schema.Type.STRING))
    );
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(BaseHttpSourceConfig.PROPERTY_URL,
           // http://confluence.atlassian.com/rest/api/space/ADMINJIRASERVER0710/content/page?limit=100&start=0
           getServerAddress() + "/rest/api/space/ADMINJIRASERVER0710/content/page?limit=2&start=0")
      .put(BaseHttpSourceConfig.PROPERTY_FORMAT, "json")
      .put(BaseHttpSourceConfig.PROPERTY_RESULT_PATH, "/results")
      .put(BaseHttpSourceConfig.PROPERTY_SCHEMA, schema.toString())
      .put(BaseHttpSourceConfig.PROPERTY_PAGINATION_TYPE, "Link in response body")
      .put(BaseHttpSourceConfig.PROPERTY_NEXT_PAGE_FIELD_PATH, "/_links/next")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/rest/api/space/ADMINJIRASERVER0710/content/page?limit=2&start=0"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testLinkInResponseBody1.txt"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/rest/api/space/ADMINJIRASERVER0710/content/page?limit=2&start=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testLinkInResponseBody2.txt"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/rest/api/space/ADMINJIRASERVER0710/content/page?limit=2&start=4"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testLinkInResponseBody3.txt"))));

    List<StructuredRecord> records = getPipelineResults(properties, 6);
    Assert.assertEquals(6, records.size());
  }

  @Test
  public void testLinkInResponseHeader() throws Exception {
    Schema schema = Schema.recordOf("etlSchemaBody",
                                    Schema.Field.of("path",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("score",
                                                    Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("repository",
                                                    Schema.recordOf("repositoryEtlRecord",
                                                                    Schema.Field.of("full_name",
                                                                                    Schema.of(Schema.Type.STRING)),
                                                                    Schema.Field.of("description",
                                                                                    Schema.of(Schema.Type.STRING)),
                                                                    Schema.Field.of("contributors_url",
                                                                                    Schema.of(Schema.Type.STRING))
                                                    ))
    );
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(BaseHttpSourceConfig.PROPERTY_URL,
           //"https://api.github.com/search/code?q=Salesforce+user:data-integrations")
           getServerAddress() + "/search/code?q=Salesforce+user:data-integrations&per_page=2")
      .put(BaseHttpSourceConfig.PROPERTY_FORMAT, "json")
      .put(BaseHttpSourceConfig.PROPERTY_RESULT_PATH, "/items")
      .put(BaseHttpSourceConfig.PROPERTY_FIELDS_MAPPING, "path:/path,score:/score,repository:/repository")
      .put(BaseHttpSourceConfig.PROPERTY_SCHEMA, schema.toString())
      .put(BaseHttpSourceConfig.PROPERTY_PAGINATION_TYPE, "Link in response header")
      .build();

    String header1 = String.format("<%1$s/search/code?q=Salesforce+user:data-integrations&per_page=2&page=2>; " +
      "rel=\"next\", <%1$s/search/code?q=Salesforce+user:data-integrations&per_page=2&page=3>; " +
      "rel=\"last\"", getServerAddress());

    String header2 = String.format("<%1$s/search/code?q=Salesforce+user:data-integrations&per_page=2&page=1>; " +
      "rel=\"prev\", <%1$s/search/code?q=Salesforce+user:data-integrations&per_page=2&page=3>; " +
      "rel=\"next\", <%1$s/search/code?q=Salesforce+user:data-integrations&per_page=2&page=3>; " +
      "rel=\"last\", <%1$s/search/code?q=Salesforce+user:data-integrations&per_page=2&page=1>; " +
      "rel=\"first\"", getServerAddress());

    String header3 = String.format("<%1$s/search/code?q=Salesforce+user:data-integrations&per_page=2&page=2>; " +
      "rel=\"prev\", <%1$s/search/code?q=Salesforce+user:data-integrations&per_page=2&page=1>; " +
      "rel=\"first\"", getServerAddress());

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/search/code?q=Salesforce+user:data-integrations&per_page=2"))
                           .willReturn(WireMock.aResponse()
                                         .withHeader("Link", header1)
                                         .withBody(readResourceFile("testLinkInResponseHeader1.txt"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/search/code?q=Salesforce+user:data-integrations&per_page=2&page=2"))
                           .willReturn(WireMock.aResponse()
                                         .withHeader("Link", header2)
                                         .withBody(readResourceFile("testLinkInResponseHeader2.txt"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/search/code?q=Salesforce+user:data-integrations&per_page=2&page=3"))
                           .willReturn(WireMock.aResponse()
                                         .withHeader("Link", header3)
                                         .withBody(readResourceFile("testLinkInResponseHeader3.txt"))));


    List<StructuredRecord> records = getPipelineResults(properties, 6);
    Assert.assertEquals(6, records.size());
  }

  @Test
  public void testNonePagination() throws Exception {
    Schema schema = Schema.recordOf("etlSchemaBody",
                                    Schema.Field.of("filename",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("language",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("url",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("name",
                                                    Schema.of(Schema.Type.STRING))
    );

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      // https://searchcode.com/api/codesearch_I/?q=cdap
      .put(BaseHttpSourceConfig.PROPERTY_URL, getServerAddress() + "/api/codesearch_I/?q=cdap")
      .put(BaseHttpSourceConfig.PROPERTY_FORMAT, "json")
      .put(BaseHttpSourceConfig.PROPERTY_RESULT_PATH, "/results")
      .put(BaseHttpSourceConfig.PROPERTY_FIELDS_MAPPING, "repo:repo,language:/language,file:/filename,url:/url")
      .put(BaseHttpSourceConfig.PROPERTY_SCHEMA, schema.toString())
      .put(BaseHttpSourceConfig.PROPERTY_PAGINATION_TYPE, "None")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/api/codesearch_I/?q=cdap"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testNonePagination1.txt"))));

    List<StructuredRecord> records = getPipelineResults(properties, 9);
    Assert.assertEquals(9, records.size());
  }

  @Test
  public void testNonePaginationBlob() throws Exception {
    Schema schema = Schema.recordOf("etlSchemaBody",
                                    Schema.Field.of("body",
                                                    Schema.of(Schema.Type.BYTES))
    );

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(BaseHttpSourceConfig.PROPERTY_URL, getServerAddress() + "/blob")
      .put(BaseHttpSourceConfig.PROPERTY_FORMAT, "blob")
      .put(BaseHttpSourceConfig.PROPERTY_SCHEMA, schema.toString())
      .put(BaseHttpSourceConfig.PROPERTY_PAGINATION_TYPE, "None")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/blob"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testNonePaginationBlob1.txt"))));

    List<StructuredRecord> records = getPipelineResults(properties, 1);
    Assert.assertEquals(1, records.size());
  }

  @Test
  public void testNonePaginationCSV() throws Exception {
    Schema schema = Schema.recordOf("etlSchemaBody",
                                    Schema.Field.of("venue",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("address",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("address2",
                                                    Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("city",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("province",
                                                    Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("zip",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("country",
                                                    Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("phone",
                                                    Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("website",
                                                    Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      // https://s3.theeventscalendar.com/uploads/2014/09/test-data-venues11.csv
      .put(BaseHttpSourceConfig.PROPERTY_URL, getServerAddress() + "/uploads/2014/09/test-data-venues11.csv")
      .put(BaseHttpSourceConfig.PROPERTY_FORMAT, "csv")
      .put(BaseHttpSourceConfig.PROPERTY_SCHEMA, schema.toString())
      .put(BaseHttpSourceConfig.PROPERTY_PAGINATION_TYPE, "None")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/uploads/2014/09/test-data-venues11.csv"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testNonePaginationCSV1.txt"))));

    List<StructuredRecord> records = getPipelineResults(properties, 13);
    Assert.assertEquals(13, records.size());
  }

  @Test
  public void testNonePaginationText() throws Exception {
    Schema schema = Schema.recordOf("etlSchemaBody",
                                    Schema.Field.of("body",
                                                    Schema.of(Schema.Type.STRING))
    );
    //"http://www.columbia.edu/~fdc/sample.html")
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(BaseHttpSourceConfig.PROPERTY_URL, getServerAddress() + "/sample.html")
      .put(BaseHttpSourceConfig.PROPERTY_FORMAT, "text")
      .put(BaseHttpSourceConfig.PROPERTY_SCHEMA, schema.toString())
      .put(BaseHttpSourceConfig.PROPERTY_PAGINATION_TYPE, "None")
      .build();

    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo("/sample.html"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testNonePaginationText1.txt"))));

    List<StructuredRecord> records = getPipelineResults(properties, 12);
    Assert.assertEquals(12, records.size());
  }

  @Test
  public void testNonePaginationXml() throws Exception {
    Schema schema = Schema.recordOf("etlSchemaBody",
                                    Schema.Field.of("name",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("type",
                                                    Schema.of(Schema.Type.STRING))
    );

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      // https://www.w3.org/2003/05/soap-envelope/
      .put(BaseHttpSourceConfig.PROPERTY_URL, getServerAddress() + "/2003/05/soap-envelope/")
      .put(BaseHttpSourceConfig.PROPERTY_FORMAT, "xml")
      .put(BaseHttpSourceConfig.PROPERTY_RESULT_PATH, "/schema/complexType[@name='Fault']/sequence/element")
      .put(BaseHttpSourceConfig.PROPERTY_FIELDS_MAPPING, "name:@name,type:@type")
      .put(BaseHttpSourceConfig.PROPERTY_SCHEMA, schema.toString())
      .put(BaseHttpSourceConfig.PROPERTY_PAGINATION_TYPE, "None")
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/2003/05/soap-envelope/"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testNonePaginationXml1.txt"))));

    List<StructuredRecord> records = getPipelineResults(properties, 5);
    Assert.assertEquals(5, records.size());
  }

  @Test
  public void testPaginationCustom() throws Exception {
    Schema schema = Schema.recordOf("etlSchemaBody",
                                    Schema.Field.of("filename",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("language",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("url",
                                                    Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("name",
                                                    Schema.of(Schema.Type.STRING))
    );

    String paginationCode = "import json\n" +
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
      "    if next_page_num == None or next_page_num > 3:\n" +
      "      return None\n" +
      "      \n" +
      String.format("    return \"%s/api/codesearch_I/?q=curl&per_page=2&p={}\".format(next_page_num)\n",
                    getServerAddress());

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      // https://searchcode.com/api/codesearch_I/?q=cdap&per_page=2
      .put(BaseHttpSourceConfig.PROPERTY_URL, getServerAddress() + "/api/codesearch_I/?q=cdap&per_page=2")
      .put(BaseHttpSourceConfig.PROPERTY_FORMAT, "json")
      .put(BaseHttpSourceConfig.PROPERTY_RESULT_PATH, "/results")
      .put(BaseHttpSourceConfig.PROPERTY_FIELDS_MAPPING, "repo:repo,language:/language,file:/filename,url:/url")
      .put(BaseHttpSourceConfig.PROPERTY_SCHEMA, schema.toString())
      .put(BaseHttpSourceConfig.PROPERTY_PAGINATION_TYPE, "Custom")
      .put(BaseHttpSourceConfig.PROPERTY_CUSTOM_PAGINATION_CODE, paginationCode)
      .build();

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/api/codesearch_I/?q=cdap&per_page=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testPaginationCustom1.txt"))));

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/api/codesearch_I/?q=curl&per_page=2&p=1"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testPaginationCustom2.txt"))));

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/api/codesearch_I/?q=curl&per_page=2&p=2"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testPaginationCustom3.txt"))));

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/api/codesearch_I/?q=curl&per_page=2&p=3"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testPaginationCustom4.txt"))));

    List<StructuredRecord> records = getPipelineResults(properties, 8);
    Assert.assertEquals(8, records.size());
  }

  protected Map<String, String> getProperties(Map<String, String> sourceProperties) {
    return new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(BaseHttpSourceConfig.PROPERTY_HTTP_METHOD, "GET")
      .put(BaseHttpSourceConfig.PROPERTY_OAUTH2_ENABLED, "false")
      .put(BaseHttpSourceConfig.PROPERTY_SERVICE_ACCOUNT_ENABLED, "false")
      .put(BaseHttpSourceConfig.PROPERTY_SERVICE_ACCOUNT_SCOPE, "")
      .put(BaseHttpSourceConfig.PROPERTY_HTTP_ERROR_HANDLING, "2..:Success,.*:Fail")
      .put(BaseHttpSourceConfig.PROPERTY_ERROR_HANDLING, "stopOnError")
      .put(BaseHttpSourceConfig.PROPERTY_RETRY_POLICY, "linear")
      .put(BaseHttpSourceConfig.PROPERTY_MAX_RETRY_DURATION, "10")
      .put(BaseHttpSourceConfig.PROPERTY_LINEAR_RETRY_INTERVAL, "1")
      .put(BaseHttpSourceConfig.PROPERTY_WAIT_TIME_BETWEEN_PAGES, "0")
      .put(BaseHttpSourceConfig.PROPERTY_CONNECT_TIMEOUT, "60")
      .put(BaseHttpSourceConfig.PROPERTY_READ_TIMEOUT, "120")
      .put(BaseHttpSourceConfig.PROPERTY_VERIFY_HTTPS, "true")
      .put(BaseHttpSourceConfig.PROPERTY_KEYSTORE_TYPE, "Java KeyStore (JKS)")
      .put(BaseHttpSourceConfig.PROPERTY_TRUSTSTORE_TYPE, "Java KeyStore (JKS)")
      .put(BaseHttpSourceConfig.PROPERTY_TRANSPORT_PROTOCOLS, "TLSv1.2")
      .putAll(sourceProperties)
      .build();
  }

  protected abstract List<StructuredRecord> getPipelineResults(Map<String, String> sourceProperties,
                                                               int expectedRecordsCount) throws Exception;

  protected String readResourceFile(String filename) throws URISyntaxException, IOException {
    return new String(Files.readAllBytes(
      Paths.get(getClass().getClassLoader().getResource(filename).toURI())));
  }

  protected String getServerAddress() {

    return "http://localhost:" + wireMockRule.port();
  }
}
