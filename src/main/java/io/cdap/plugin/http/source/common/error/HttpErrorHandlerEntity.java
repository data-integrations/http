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
package io.cdap.plugin.http.source.common.error;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Represents a pair of values:
 * - pattern which the http code is matched against
 * - error handling strategy used for responses with that http code.
 */
public class HttpErrorHandlerEntity {
  private final Pattern pattern;
  private final HttpErrorHandlingStrategy strategy;

  public HttpErrorHandlerEntity(Pattern pattern, HttpErrorHandlingStrategy strategy) {
    this.pattern = pattern;
    this.strategy = strategy;
  }

  public Pattern getPattern() {
    return pattern;
  }

  public HttpErrorHandlingStrategy getStrategy() {
    return strategy;
  }

  @Override
  public boolean equals(Object o) {
    HttpErrorHandlerEntity that = (HttpErrorHandlerEntity) o;
    return Objects.equals(pattern, that.pattern) && strategy == that.strategy;
  }

  @Override
  public int hashCode() {
    return Objects.hash(pattern, strategy);
  }
}
