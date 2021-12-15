/*
 * Copyright © 2021 Cask Data, Inc.
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
package io.cdap.plugin.http.action;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.http.common.RetryPolicy;
import io.cdap.plugin.http.common.error.HttpErrorHandler;
import io.cdap.plugin.http.common.error.RetryableErrorHandling;
import io.cdap.plugin.http.common.http.HttpClient;
import io.cdap.plugin.http.common.http.HttpConstants;
import io.cdap.plugin.http.common.http.HttpResponse;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.awaitility.pollinterval.FixedPollInterval;
import org.awaitility.pollinterval.IterativePollInterval;
import org.awaitility.pollinterval.PollInterval;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * HTTP Action Plugin
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(HttpConstants.HTTP_PLUGIN_NAME)
@Description("Action that runs a MySQL command")
public class HttpAction extends Action {

  private final HttpActionConfig config;

  public HttpAction(HttpActionConfig config) {
    this.config = config;
  }

    private boolean makeRequest(HttpClient httpClient, String uri, HttpErrorHandler errorHandler) throws IOException {
    HttpResponse response = new HttpResponse(httpClient.executeHTTP(uri));
    RetryableErrorHandling errorHandlingStrategy = errorHandler.getErrorHandlingStrategy(response.getStatusCode());

    response.close();
    return !errorHandlingStrategy.shouldRetry();
  }

  @Override
  public void run(ActionContext actionContext) throws Exception {
    try (HttpClient httpClient = new HttpClient(config)) {
      HttpErrorHandler httpErrorHandler = new HttpErrorHandler(config);
      PollInterval pollInterval = (config.getRetryPolicy().equals(RetryPolicy.LINEAR))
        ? FixedPollInterval.fixed(Objects.requireNonNull(config.getLinearRetryInterval()), TimeUnit.SECONDS)
        : IterativePollInterval.iterative(duration -> duration.multiply(2));

      Awaitility
        .await().with()
        .pollInterval(pollInterval)
        .timeout(config.getMaxRetryDuration(), TimeUnit.SECONDS)
        .until(() -> makeRequest(httpClient, config.getUrl(), httpErrorHandler));
    } catch (ConditionTimeoutException ignored) {
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
  }
}
