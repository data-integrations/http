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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.http.source.batch.HttpBatchSource;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class BaseHttpBatchSourceETLTest extends HydratorTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(BaseHttpBatchSourceETLTest.class);

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Rule
  public TestName name = new TestName();

  @Rule
  public WireMockRule wireMockRule = new WireMockRule();

  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the artifact as its parent.
    // this will make our plugins available.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      HttpBatchSource.class);
  }

  public List<StructuredRecord> getPipelineResults(Map<String, String> sourceProperties) throws Exception {
    Map<String, String> allProperties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", name.getMethodName())
      .put(BaseHttpSourceConfig.PROPERTY_HTTP_METHOD, "GET")
      .put(BaseHttpSourceConfig.PROPERTY_OAUTH2_ENABLED, "false")
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

    ETLStage source = new ETLStage("HttpReader", new ETLPlugin("HTTP", BatchSource.PLUGIN_TYPE,
                                                               allProperties, null));

    String outputDatasetName = "output-batchsourcetest_" + name.getMethodName();
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app("HttpBatch_" + name.getMethodName());
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED,  5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    return outputRecords;
  }

  protected String readResourceFile(String filename) throws URISyntaxException, IOException {
    return new String(Files.readAllBytes(
      Paths.get(getClass().getClassLoader().getResource(filename).toURI())));
  }

  protected String getServerAddress() {

    return "http://localhost:" + wireMockRule.port();
  }
}
