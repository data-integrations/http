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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonElement;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.sink.batch.HTTPSinkConfig;
import io.cdap.plugin.http.common.pagination.page.JSONUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;


/**
 * A class which contains utilities to make OAuth2 specific calls.
 */
public class OAuthUtil {
  public static String getAccessTokenByRefreshToken(CloseableHttpClient httpclient, String tokenUrl, String clientId,
                                                    String clientSecret, String refreshToken)
    throws IOException {

    URI uri;
    try {
      uri = new URIBuilder(tokenUrl)
        .setParameter("client_id", clientId)
        .setParameter("client_secret", clientSecret)
        .setParameter("refresh_token", refreshToken)
        .setParameter("grant_type", "refresh_token")
        .build();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to build token URI for OAuth2", e);
    }

    HttpPost httppost = new HttpPost(uri);
    CloseableHttpResponse response = httpclient.execute(httppost);
    String responseString = EntityUtils.toString(response.getEntity(), "UTF-8");

    JsonElement jsonElement = JSONUtil.toJsonObject(responseString).get("access_token");
    if (jsonElement == null) {
         throw new IllegalArgumentException("Access token not found");
    }
    return jsonElement.getAsString();
  }

  public static String getAccessToken(String serviceAccountScope, Boolean isServiceAccountJson,
                                      String serviceAccountJson, Boolean isServiceAccountFilePath,
                                      String serviceAccountFilePath) throws IOException {
    GoogleCredentials credential;
    String accessToken = "";
    try {
      ImmutableSet scopeSet = ImmutableSet.of("https://www.googleapis.com/auth/cloud-platform");
      if (serviceAccountScope != null) {
        String[] scopes = serviceAccountScope.split("\n");
        for (String scope: scopes) {
          scopeSet = ImmutableSet.builder().addAll(scopeSet).add(scope).build();
        }
      }
      if (isServiceAccountJson) {
        InputStream jsonInputStream = new ByteArrayInputStream(serviceAccountJson
                .getBytes(StandardCharsets.UTF_8));
        credential = GoogleCredentials.fromStream(jsonInputStream)
                .createScoped(scopeSet);
      } else if (isServiceAccountFilePath && !Strings.isNullOrEmpty(serviceAccountFilePath)
              && !BaseHttpSourceConfig.PROPERTY_AUTO_DETECT_VALUE.equals(serviceAccountFilePath)) {
        credential = GoogleCredentials.fromStream(new FileInputStream(serviceAccountFilePath))
                .createScoped(scopeSet);
      } else {
        credential = GoogleCredentials.getApplicationDefault()
                .createScoped(scopeSet);
      }
      accessToken = credential.refreshAccessToken().getTokenValue();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to generate Access Token with given Service Account information", e);
    }
    return accessToken;
  }
}

