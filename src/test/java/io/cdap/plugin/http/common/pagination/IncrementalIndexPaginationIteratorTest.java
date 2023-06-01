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
package io.cdap.plugin.http.common.pagination;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.plugin.http.source.batch.HttpBatchSourceConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class IncrementalIndexPaginationIteratorTest {

    private final String configJsonWithPaginationIndex = "{\"url\":\"https://abc.com/path1?page" +
            "\\u003d{pagination.index}\",\"httpMethod\":\"GET\",\"format\":\"json\",\"httpErrorsHandling\":\"" +
            "2..:Success,.*:Fail\",\"errorHandling\":\"stopOnError\",\"retryPolicy\":\"exponential\"," +
            "\"linearRetryInterval\":30,\"maxRetryDuration\":600,\"connectTimeout\":120,\"readTimeout\":120," +
            "\"paginationType\":\"Increment an index\",\"startIndex\":1,\"maxIndex\":3,\"indexIncrement\":1," +
            "\"verifyHttps\":\"true\",\"keystoreType\":\"Java KeyStore (JKS)\",\"keystoreKeyAlgorithm\":\"SunX509\"," +
            "\"trustStoreType\":\"Java KeyStore (JKS)\",\"trustStoreKeyAlgorithm\":\"SunX509\"," +
            "\"transportProtocols\":\"TLSv1.2\",\"schema\":\"{\\\"name\\\":\\\"etlSchemaBody\\\"," +
            "\\\"type\\\":\\\"record\\\",\\\"fields\\\":[{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"string\\\"," +
            "\\\"null\\\"]}]}\",\"authType\":\"basicAuth\",\"oauth2Enabled\":\"false\",\"username\":\"username\"," +
            "\"password\":\"password123\",\"referenceName\":\"input\",\"properties\":{\"properties\":" +
            "{\"schema\":\"{\\\"name\\\":\\\"etlSchemaBody\\\",\\\"type\\\":\\\"record\\\",\\\"fields\\\":" +
            "[{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]}\",\"retryPolicy\":" +
            "\"exponential\",\"maxRetryDuration\":\"600\",\"httpMethod\":\"GET\",\"startIndex\":\"1\",\"password\"" +
            ":\"password\",\"maxIndex\":\"3\",\"indexIncrement\":\"1\",\"linearRetryInterval\":\"30\"," +
            "\"connectTimeout\":\"120\",\"trustStoreType\":\"Java KeyStore (JKS)\",\"authType\":\"basicAuth\"," +
            "\"referenceName\":\"input\",\"format\":\"json\",\"verifyHttps\":\"true\",\"trustStoreKeyAlgorithm\":" +
            "\"SunX509\",\"transportProtocols\":\"TLSv1.2\",\"url\"" +
            ":\"https://abc.com/path1?page\\u003d{pagination.index}\"," +
            "\"paginationType\":\"Increment an index\",\"keystoreType\":\"Java KeyStore (JKS)\"," +
            "\"readTimeout\":\"120\",\"oauth2Enabled\":\"false\",\"httpErrorsHandling\":\"2..:Success,.*:Fail\"," +
            "\"errorHandling\":\"stopOnError\",\"keystoreKeyAlgorithm\":\"SunX509\",\"username\":\"user\"}," +
            "\"macros\":{\"lookupProperties\":[],\"macroFunctions\":[]}},\"rawProperties\":{\"properties\":" +
            "{\"schema\":\"{\\\"name\\\":\\\"etlSchemaBody\\\",\\\"type\\\":\\\"record\\\",\\\"fields\\\":" +
            "[{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]}\",\"retryPolicy\":" +
            "\"exponential\",\"maxRetryDuration\":\"600\",\"httpMethod\":\"GET\",\"startIndex\":\"1\",\"password\":" +
            "\"password\",\"maxIndex\":\"3\",\"indexIncrement\":\"1\",\"linearRetryInterval\":\"30\"," +
            "\"connectTimeout\":\"120\",\"trustStoreType\":\"Java KeyStore (JKS)\",\"authType\":\"basicAuth\"," +
            "\"referenceName\":\"input\",\"format\":\"json\",\"verifyHttps\":\"true\",\"trustStoreKeyAlgorithm\"" +
            ":\"SunX509\",\"transportProtocols\":\"TLSv1.2\",\"url\":" +
            "\"https://abc.com/path1?page\\u003d{pagination.index}\"," +
            "\"paginationType\":\"Increment an index\",\"keystoreType\":\"Java KeyStore (JKS)\"," +
            "\"readTimeout\":\"120\",\"oauth2Enabled\":\"false\",\"httpErrorsHandling\":\"2..:Success,.*:Fail\"," +
            "\"errorHandling\":\"stopOnError\",\"keystoreKeyAlgorithm\":\"SunX509\",\"username\":\"user\"}," +
            "\"macros\":{\"lookupProperties\":[],\"macroFunctions\":[]}},\"macroFields\":[]}";

    private static final Gson gson = new GsonBuilder().create();

    @Test
    public void testHttpClientCreationForBasicAuth() {
        HttpBatchSourceConfig httpBatchSourceConfig =
                gson.fromJson(configJsonWithPaginationIndex, HttpBatchSourceConfig.class);

        BaseHttpPaginationIterator httpPaginationIterator =
                PaginationIteratorFactory.createInstance(httpBatchSourceConfig, null);
        Assert.assertEquals("https://abc.com/path1?page=1", httpPaginationIterator.nextPageUrl);
        try {
            CloseableHttpClient client = httpPaginationIterator.getHttpClient()
                    .createHttpClient(httpPaginationIterator.nextPageUrl);
            Assert.assertNotNull(client);
        } catch (IOException exception) {
            Assert.fail("Error in validating Incremental Pagination Iterator.");
        }
    }
}
