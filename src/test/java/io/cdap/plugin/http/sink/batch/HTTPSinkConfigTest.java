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

package io.cdap.plugin.http.sink.batch;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link HTTPSinkConfig}
 */
public class HTTPSinkConfigTest {
  private static final String MOCK_STAGE = "mockStage";

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
    true
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidUrl() {
    HTTPSinkConfig config = HTTPSinkConfig.newBuilder(VALID_CONFIG)
      .setUrl("abc")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertPropertyValidationFailed(failureCollector, HTTPSinkConfig.URL);
  }

  @Test
  public void testInvalidConnectionTimeout() {
    HTTPSinkConfig config = HTTPSinkConfig.newBuilder(VALID_CONFIG)
      .setConnectTimeout(-1)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertPropertyValidationFailed(failureCollector, HTTPSinkConfig.CONNECTION_TIMEOUT);
  }

  @Test
  public void testInvalidRequestHeaders() {
    HTTPSinkConfig config = HTTPSinkConfig.newBuilder(VALID_CONFIG)
      .setRequestHeaders("abc")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertPropertyValidationFailed(failureCollector, HTTPSinkConfig.REQUEST_HEADERS);
  }

  @Test
  public void testInvalidMethod() {
    HTTPSinkConfig config = HTTPSinkConfig.newBuilder(VALID_CONFIG)
      .setMethod("abc")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertPropertyValidationFailed(failureCollector, HTTPSinkConfig.METHOD);
  }

  @Test
  public void testInvalidNumRetries() {
    HTTPSinkConfig config = HTTPSinkConfig.newBuilder(VALID_CONFIG)
      .setNumRetries(-1)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertPropertyValidationFailed(failureCollector, HTTPSinkConfig.NUM_RETRIES);
  }

  @Test
  public void testInvalidReadTimeout() {
    HTTPSinkConfig config = HTTPSinkConfig.newBuilder(VALID_CONFIG)
      .setReadTimeout(-1)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertPropertyValidationFailed(failureCollector, HTTPSinkConfig.READ_TIMEOUT);
  }

  @Test
  public void testInvalidMessageFormat() {
    HTTPSinkConfig config = HTTPSinkConfig.newBuilder(VALID_CONFIG)
      .setMessageFormat("Custom")
      .setBody(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertPropertyValidationFailed(failureCollector, HTTPSinkConfig.MESSAGE_FORMAT);
  }

  @Test(expected = ValidationException.class)
  public void testHTTPSinkWithEmptyUrl() {
    HTTPSinkConfig config = HTTPSinkConfig.newBuilder(VALID_CONFIG)
      .setUrl("")
      .build();

    MockFailureCollector collector = new MockFailureCollector("httpsinkwithemptyurl");
    config.validate(collector);
    collector.getOrThrowException();
  }

  @Test()
  public void testValidInputSchema() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    HTTPSinkConfig config = HTTPSinkConfig.newBuilder(VALID_CONFIG).build();
    MockFailureCollector collector = new MockFailureCollector("httpsinkwithvalidinputschema");
    config.validateSchema(schema, collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  public static void assertPropertyValidationFailed(MockFailureCollector failureCollector, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList());
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(paramName, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}
