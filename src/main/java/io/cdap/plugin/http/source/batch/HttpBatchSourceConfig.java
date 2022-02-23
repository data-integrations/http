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
package io.cdap.plugin.http.source.batch;

import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;

/**
 * Provides all the configurations required for configuring the {@link HttpBatchSource} plugin.
 */
public class HttpBatchSourceConfig extends BaseHttpSourceConfig {
  protected HttpBatchSourceConfig(String referenceName) {
    super(referenceName);
  }

  private HttpBatchSourceConfig(HttpBatchSourceConfigBuilder builder) {
    super(builder.referenceName);
    this.url = builder.url;
    this.httpMethod = builder.httpMethod;
    this.headers = builder.headers;
    this.format = builder.format;
    this.oauth2Enabled = builder.oauth2Enabled;
    this.errorHandling = builder.errorHandling;
    this.retryPolicy = builder.retryPolicy;
    this.maxRetryDuration = builder.maxRetryDuration;
    this.connectTimeout = builder.connectTimeout;
    this.readTimeout = builder.readTimeout;
    this.paginationType = builder.paginationType;
    this.verifyHttps = builder.verifyHttps;
  }

  public static HttpBatchSourceConfigBuilder builder() {
    return new HttpBatchSourceConfigBuilder();
  }

  /**
   * Builder for HttpBatchSourceConfig
   */
  public static class HttpBatchSourceConfigBuilder {

    private String referenceName;
    private String url;
    private String httpMethod;
    private String headers;
    private String format;
    private String oauth2Enabled;
    private String errorHandling;
    private String retryPolicy;
    private Long maxRetryDuration;
    private Integer connectTimeout;
    private Integer readTimeout;
    private String paginationType;
    private String verifyHttps;

    public HttpBatchSourceConfigBuilder setReferenceName (String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public HttpBatchSourceConfigBuilder setUrl(String url) {
      this.url = url;
      return this;
    }

    public HttpBatchSourceConfigBuilder setHttpMethod(String httpMethod) {
      this.httpMethod = httpMethod;
      return this;
    }

    public HttpBatchSourceConfigBuilder setHeaders(String headers) {
      this.headers = headers;
      return this;
    }

    public HttpBatchSourceConfigBuilder setFormat(String format) {
      this.format = format;
      return this;
    }

    public HttpBatchSourceConfigBuilder setOauth2Enabled(String oauth2Enabled) {
      this.oauth2Enabled = oauth2Enabled;
      return this;
    }

    public HttpBatchSourceConfigBuilder setErrorHandling(String errorHandling) {
      this.errorHandling = errorHandling;
      return this;
    }

    public HttpBatchSourceConfigBuilder setRetryPolicy(String retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }

    public HttpBatchSourceConfigBuilder setMaxRetryDuration(Long maxRetryDuration) {
      this.maxRetryDuration = maxRetryDuration;
      return this;
    }

    public HttpBatchSourceConfigBuilder setConnectTimeout(Integer connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public HttpBatchSourceConfigBuilder setReadTimeout(Integer readTimeout) {
      this.readTimeout = readTimeout;
      return this;
    }

    public HttpBatchSourceConfigBuilder setPaginationType(String paginationType) {
      this.paginationType = paginationType;
      return this;
    }

    public HttpBatchSourceConfigBuilder setVerifyHttps(String verifyHttps) {
      this.verifyHttps = verifyHttps;
      return this;
    }

    public HttpBatchSourceConfig build() {
      return new HttpBatchSourceConfig(this);
    }
  }
}
