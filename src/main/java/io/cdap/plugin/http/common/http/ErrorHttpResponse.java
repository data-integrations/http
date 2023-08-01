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

import org.apache.http.Header;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Empty Http Response to signify a Response object with empty data.
 */
public class ErrorHttpResponse extends HttpResponse {

  private int statusCode;

  public ErrorHttpResponse(int statusCode) {
    super(null);
    this.statusCode = statusCode;
  }

  @Override
  public int getStatusCode() {
    return statusCode;
  }

  @Override
  public Header[] getAllHeaders() {
    return new Header[0];
  }

  @Override
  public Header getFirstHeader(String headerName) {
    return null;
  }

  @Override
  public InputStream getInputStream() {
    return new ByteArrayInputStream(new byte[0]);
  }

  @Override
  public String getBody() {
    return "";
  }

  @Override
  public byte[] getBytes() {
    return new byte[0];
  }

  @Override
  public void close() throws IOException {
  }
}
