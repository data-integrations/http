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
package io.cdap.plugin.http.source.common.http;

import com.google.gson.JsonElement;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.pagination.page.JSONUtil;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static io.cdap.plugin.http.source.common.http.OAuthGrantType.CLIENT_CREDENTIALS;
import static io.cdap.plugin.http.source.common.http.OAuthGrantType.REFRESH_TOKEN;

/**
 * A class which contains utilities to make OAuth2 specific calls.
 */
public class OAuthUtil {

  public static String getAccessToken(CloseableHttpClient httpclient, BaseHttpSourceConfig config) throws IOException {
    switch (config.getOauth2GrantType()) {
      case REFRESH_TOKEN:
        return getAccessTokenByRefreshToken(httpclient, config.getTokenUrl(),
                config.getClientId(), config.getClientSecret(),
                config.getRefreshToken());
      case CLIENT_CREDENTIALS:
        return getAccessTokenByClientCredentials(httpclient, config.getTokenUrl(),
                config.getClientId(), config.getClientSecret(), config.getScopes());
      default:
        throw new IOException("Invalid Grant Type. Cannot retrieve access token.");
    }
  }


  public static String getAccessTokenByRefreshToken(CloseableHttpClient httpclient, String tokenUrl, String clientId,
                                                    String clientSecret, String refreshToken)
    throws IOException {

    URI uri;
    try {
      uri = new URIBuilder(tokenUrl)
        .setParameter("client_id", clientId)
        .setParameter("client_secret", clientSecret)
        .setParameter("refresh_token", refreshToken)
        .setParameter("grant_type", OAuthGrantType.REFRESH_TOKEN.getValue())
        .build();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to build access token URI for OAuth2 with grant type = " +
              OAuthGrantType.REFRESH_TOKEN.getValue(), e);
    }

    HttpPost httppost = new HttpPost(uri);
    CloseableHttpResponse response = httpclient.execute(httppost);
    String responseString = EntityUtils.toString(response.getEntity(), "UTF-8");

    JsonElement jsonElement = JSONUtil.toJsonObject(responseString).get("access_token");
    return jsonElement.getAsString();
  }

  private static String getAccessTokenByClientCredentials(CloseableHttpClient httpclient, String tokenUrl,
                                                          String clientId, String clientSecret, String scope)
          throws IOException {
    URI uri;
    try {
      uri = new URIBuilder(tokenUrl)
              .build();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to build access token URI for OAuth2 with grant type = " +
              OAuthGrantType.CLIENT_CREDENTIALS.getValue(), e);
    }

    HttpPost httppost = new HttpPost(uri);
    List<BasicNameValuePair> nameValuePairs = new ArrayList<>();
    nameValuePairs.add(new BasicNameValuePair("scope", scope));
    nameValuePairs.add(new BasicNameValuePair("grant_type", OAuthGrantType.CLIENT_CREDENTIALS.getValue()));

    httppost.setEntity(new UrlEncodedFormEntity(nameValuePairs));

    String authorizationKey = "Basic " + Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes());

    httppost.addHeader(new BasicHeader("Authorization", authorizationKey));

    CloseableHttpResponse response = httpclient.execute(httppost);
    String responseString = EntityUtils.toString(response.getEntity(), "UTF-8");


    JsonElement jsonElement = JSONUtil.toJsonObject(responseString).get("access_token");
    return jsonElement.getAsString();
  }
}

