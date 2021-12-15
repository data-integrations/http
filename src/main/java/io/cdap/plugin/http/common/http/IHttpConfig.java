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
package io.cdap.plugin.http.common.http;

import io.cdap.plugin.http.common.RetryPolicy;
import io.cdap.plugin.http.common.error.ErrorHandling;
import io.cdap.plugin.http.common.error.HttpErrorHandlerEntity;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Interface for the HTTP Plugin Config
 */
public interface IHttpConfig {

  String getUrl();

  String getHttpMethod();

  @Nullable
  String getHeaders();

  @Nullable
  String getRequestBody();

  @Nullable
  String getUsername();

  @Nullable
  String getPassword();

  @Nullable
  String getProxyUrl();

  @Nullable
  String getProxyUsername();

  @Nullable
  String getProxyPassword();

  ErrorHandling getErrorHandling();

  List<HttpErrorHandlerEntity> getHttpErrorHandlingEntries();

  Long getLinearRetryInterval();

  Long getMaxRetryDuration();

  RetryPolicy getRetryPolicy();

  Integer getConnectTimeout();

  Integer getReadTimeout();

  Boolean getOauth2Enabled();

  @Nullable
  String getAuthUrl();

  @Nullable
  String getTokenUrl();

  @Nullable
  String getClientId();

  @Nullable
  String getClientSecret();

  @Nullable
  String getRefreshToken();

  Boolean getVerifyHttps();

  @Nullable
  String getKeystoreFile();

  @Nullable
  KeyStoreType getKeystoreType();

  @Nullable
  String getKeystorePassword();

  @Nullable
  String getKeystoreKeyAlgorithm();

  @Nullable
  String getTrustStoreFile();

  @Nullable
  KeyStoreType getTrustStoreType();

  @Nullable
  String getTrustStorePassword();

  @Nullable
  String getTrustStoreKeyAlgorithm();

  @Nullable
  String getCipherSuites();

  @Nullable
  String getKeystoreCertAliasName();

  @Nullable
  Map<String, String> getHeadersMap();

  List<String> getTransportProtocolsList();

  void validate();
}
