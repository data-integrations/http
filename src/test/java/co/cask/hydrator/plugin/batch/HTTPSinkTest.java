/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import co.cask.hydrator.plugin.mock.MockFeedHandler;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.HttpMethod;

/**
 * Test for HTTP Sink
 */
public class HTTPSinkTest extends HydratorTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId BATCH_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "4.0.0");
  protected static final ArtifactSummary BATCH_ARTIFACT = new ArtifactSummary("data-pipeline", "4.0.0");
  private static final Schema inputSchema = Schema.recordOf(
    "input-record",
    Schema.Field.of("id", Schema.of(Schema.Type.STRING)));
  private static NettyHttpService httpService;
  protected static String baseURL;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    setupBatchArtifacts(BATCH_ARTIFACT_ID, DataPipelineApp.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("http-sink-plugin", "1.0.0"), BATCH_ARTIFACT_ID,
                      HTTPSink.class);
    List<HttpHandler> handlers = new ArrayList<>();
    handlers.add(new MockFeedHandler());
    httpService = NettyHttpService.builder("MockService").setHttpHandlers(handlers).build();
    httpService.start();
    int port = httpService.getBindAddress().getPort();
    baseURL = "http://localhost:" + port;
    URL setPortURL = new URL(baseURL + "/feeds/users");
    HttpURLConnection urlConn = (HttpURLConnection) setPortURL.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod(HttpMethod.PUT);
    urlConn.getOutputStream().write("samuel jackson, dwayne johnson, christopher walken".getBytes(Charsets.UTF_8));
    Assert.assertEquals(200, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  @AfterClass
  public static void teardown() throws Exception {
    httpService.stop();
  }

  @After
  public void verifyandcleanupTest() throws IOException {
    getFeeds();
    resetFeeds();
  }

  @Test
  public void testHTTPSink() throws Exception {
    String inputDatasetName = "input-http-sink";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("url", baseURL + "/feeds/users")
      .put("method", "PUT")
      .put("messageFormat", "Custom")
      .put("charset", "UTF-8")
      .put("body", "cask cdap, hydrator tracker, ui cli")
      .put("batchSize", "1")
      .put("referenceName", "HTTPSinkReference")
      .put("delimiterForMessages", "\n")
      .put("numRetries", "3")
      .put("followRedirects", "true")
      .put("disableSSLValidation", "true")
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("failOnNon200Response", "true")
      .build();

    ETLStage sink = new ETLStage("HTTP", new ETLPlugin("HTTP", BatchSink.PLUGIN_TYPE, properties, null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("httpsinktest");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("id", "1").build()
    );
    MockSource.writeInput(inputManager, input);
    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.start();
    manager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
  }

  private int resetFeeds() throws IOException {
    URL url = new URL(baseURL + "/feeds");
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod(HttpMethod.DELETE);
    int responseCode = urlConn.getResponseCode();
    urlConn.disconnect();
    return responseCode;
  }

  private int getFeeds() throws IOException {
    URL url = new URL(baseURL + "/feeds/users/");
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setRequestMethod(HttpMethod.GET);
    int responseCode = urlConn.getResponseCode();
    BufferedReader reader = new BufferedReader(new InputStreamReader(urlConn.getInputStream()));
    StringBuilder result = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      result.append(line);
    }
    Assert.assertEquals(200, responseCode);
    Assert.assertEquals("cask cdap, hydrator tracker, ui cli", result.toString());
    urlConn.disconnect();
    return responseCode;
  }
}
