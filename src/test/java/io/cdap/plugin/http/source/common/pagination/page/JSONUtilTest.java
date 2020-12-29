/*
 * Copyright © 2019-2020 Cask Data, Inc.
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

import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class JSONUtilTest {
  private static final String JSON = "\n" +
    "{\n" +
    " \"pageInfo\": {\n" +
    "  \"totalResults\": 208,\n" +
    "  \"resultsPerPage\": 2\n" +
    " },\n" +
    " \"items\": [\n" +
    "  {\n" +
    "   \"kind\": \"youtube#searchResult\",\n" +
    "   \"etag\": \"\\\"Bdx4f4ps3xCOOo1WZ91nTLkRZ_c/yrJNwvacPS7tA7BQCQmeIZr7fg8\\\"\",\n" +
    "   \"id\": {\n" +
    "    \"kind\": \"youtube#channel\",\n" +
    "    \"channelId\": \"UCfkRcekMTa5GA2DdNKba7Jg\"\n" +
    "   },\n" +
    "   \"snippet\": {\n" +
    "    \"publishedAt\": \"2015-02-12T22:12:43.000Z\",\n" +
    "    \"channelId\": \"UCfkRcekMTa5GA2DdNKba7Jg\",\n" +
    "    \"title\": \"Cask\",\n" +
    "    \"description\": \"Founded by developers for developers, Cask is an open source big data software...\",\n" +
    "    \"thumbnails\": {\n" +
    "    },\n" +
    "    \"channelTitle\": \"Cask\",\n" +
    "    \"liveBroadcastContent\": \"upcoming\"\n" +
    "   }\n" +
    "  },\n" +
    "  {\n" +
    "   \"kind\": \"youtube#searchResult\",\n" +
    "   \"etag\": \"\\\"Bdx4f4ps3xCOOo1WZ91nTLkRZ_c/uv6u8PSG0DsOqN9m77o06Jl4LnA\\\"\",\n" +
    "   \"id\": {\n" +
    "    \"kind\": \"youtube#video\",\n" +
    "    \"videoId\": \"ntOXeYecj7o\"\n" +
    "   },\n" +
    "   \"snippet\": {\n" +
    "    \"publishedAt\": \"2016-12-21T19:32:03.000Z\",\n" +
    "    \"channelId\": \"UCfkRcekMTa5GA2DdNKba7Jg\",\n" +
    "    \"title\": \"Cask Product Tour\",\n" +
    "    \"description\": \"In this video, we take you on a product tour of CDAP ...\",\n" +
    "    \"thumbnails\": {\n" +
    "    },\n" +
    "    \"channelTitle\": \"Cask\",\n" +
    "    \"liveBroadcastContent\": \"none\"\n" +
    "   }\n" +
    "  }\n" +
    " ]\n" +
    "}";

  @Test
  public void testRetrievePartial() {
    JsonObject jsonObject = JSONUtil.toJsonObject(JSON);
    JSONUtil.JsonQueryResponse response = JSONUtil.getJsonElementByPath(jsonObject, "/items/snippet",
                                                                        new ArrayList<>());
    Assert.assertEquals(response.getRetrievedPath(), "/items");
    Assert.assertEquals(response.getUnretrievedPath(), "/snippet");
    Assert.assertEquals("UCfkRcekMTa5GA2DdNKba7Jg", response.getAsJsonArray().get(0).getAsJsonObject()
      .get("snippet").getAsJsonObject().get("channelId").getAsString());
  }

  @Test
  public void testRetrievePrimitive() {
    JsonObject jsonObject = JSONUtil.toJsonObject(JSON);
    JSONUtil.JsonQueryResponse response = JSONUtil.getJsonElementByPath(jsonObject, "/pageInfo/totalResults",
                                                                        new ArrayList<>());
    Assert.assertEquals("/pageInfo/totalResults", response.getRetrievedPath());
    Assert.assertEquals("/", response.getUnretrievedPath());
    Assert.assertEquals(208, response.getAsJsonPrimitive().getAsInt());
  }

  @Test
  public void testRetrieveObject() {
    JsonObject jsonObject = JSONUtil.toJsonObject(JSON);
    JSONUtil.JsonQueryResponse response = JSONUtil.getJsonElementByPath(jsonObject, "/pageInfo",
                                                                        new ArrayList<>());
    Assert.assertEquals("/pageInfo", response.getRetrievedPath());
    Assert.assertEquals("/", response.getUnretrievedPath());
    Assert.assertEquals(2, response.getAsJsonObject().get("resultsPerPage").getAsInt());
  }
}
