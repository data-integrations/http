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

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * A response from an http endpoint.
 */
public class HttpResponse implements Closeable {
  private CloseableHttpResponse response;
  private byte[] bytes;
  private String body;

  public HttpResponse(CloseableHttpResponse response) {
    this.response = response;
  }

  public int getStatusCode() {
    return response.getStatusLine().getStatusCode();
  }

  public Header[] getAllHeaders() {
    return response.getAllHeaders();
  }

  public Header getFirstHeader(String headerName) {
    return response.getFirstHeader(headerName);
  }

  public InputStream getInputStream() throws IOException {
    if (bytes != null) { // CloseableHttpResponse already read and closed the stream. So we need to use existing body.
      return new ByteArrayInputStream(bytes);
    } else {
      HttpEntity responseEntity = response.getEntity();
      return responseEntity.getContent();
    }
  }

  /**
   * Throws an exception is response stream has already been read.
   *
   * @return body of request
   */
  public String getBody() {
    if (body == null) {
      body = new String(getBytes(), StandardCharsets.UTF_8);
    }
    return body;
  }

  /**
   * Throws an exception is response stream has already been read.
   *
   * @return bytes body of request
   */
  public byte[] getBytes() {
    if (bytes == null) {
      HttpEntity responseEntity = response.getEntity();
      if (responseEntity == null) {
        return new byte[0];
      }
      try {
        bytes = EntityUtils.toByteArray(responseEntity);
      } catch (IOException e) { // this method is used in multiple next() methods where IOException is not in "throws"
        throw new RuntimeException("Cannot read bytes content of the page", e);
      }
    }
    return bytes;
  }

  @Override
  public void close() throws IOException {
    if (response != null) {
      response.close();
    }
  }
}

