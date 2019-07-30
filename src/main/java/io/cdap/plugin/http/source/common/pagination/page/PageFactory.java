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
package io.cdap.plugin.http.source.common.pagination.page;

import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.error.HttpErrorHandler;
import io.cdap.plugin.http.source.common.http.HttpResponse;

import java.io.IOException;

/**
 * A factory which creates instance of {@BasePage} in accordance to format configured in input config.
 * If erroneous page is being handled, {@HttpErrorPage} is returned, which returns a single error entry.
 */
public class PageFactory {
  public static BasePage createInstance(BaseHttpSourceConfig config, HttpResponse httpResponse,
                                        HttpErrorHandler httpErrorHandler, boolean isError) throws IOException {
    if (isError) {
      return new HttpErrorPage(config, httpResponse, httpErrorHandler);
    }

    switch(config.getFormat()) {
      case JSON:
        return new JsonPage(config, httpResponse);
      case XML:
        return new XmlPage(config, httpResponse);
      case TSV:
        return new DelimitedPage(config, httpResponse, "\t");
      case CSV:
        return new DelimitedPage(config, httpResponse, ",");
      case TEXT:
        return new TextPage(config, httpResponse);
      case BLOB:
        return new BlobPage(config, httpResponse);
      default:
        throw new IllegalArgumentException(String.format("Unsupported page format: '%s'", config.getFormat()));
    }
  }
}
