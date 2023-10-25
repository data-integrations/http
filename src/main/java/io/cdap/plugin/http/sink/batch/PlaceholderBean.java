/*
 * Copyright © 2023 Cask Data, Inc.
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

/**
 * This class stores the placeholder information to avoid performing string functions for each record.
 */
public class PlaceholderBean {
  private static final String PLACEHOLDER_FORMAT = "#%s";
  private final String placeHolderKey;
  private final String placeHolderKeyWithPrefix;
  private final int startIndex;
  private final int endIndex;

  public PlaceholderBean(String url, String placeHolderKey) {
    this.placeHolderKey = placeHolderKey;
    this.placeHolderKeyWithPrefix = String.format(PLACEHOLDER_FORMAT, placeHolderKey);
    this.startIndex = url.indexOf(placeHolderKeyWithPrefix);
    this.endIndex = startIndex + placeHolderKeyWithPrefix.length();
  }

  public String getPlaceHolderKey() {
    return placeHolderKey;
  }

  public int getStartIndex() {
    return startIndex;
  }

  public int getEndIndex() {
    return endIndex;
  }
}
