/*
 * Copyright © 2024 Cask Data, Inc.
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
package io.cdap.plugin.http.source.common;

import io.cdap.plugin.http.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Unit test for {@link RawStringPerLine}
 */
public class RawStringPerLineTest {

  @Mock
  HttpResponse httpResponse;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    String sampleContent = "Line 1\nLine 2\n";
    InputStream inputStream = new ByteArrayInputStream(sampleContent.getBytes());
    Mockito.when(httpResponse.getInputStream()).thenReturn(inputStream);
  }

  @Test
  public void testRawStringPerLine() {
    RawStringPerLine rawStringPerLine = new RawStringPerLine(httpResponse);

    Assert.assertTrue(rawStringPerLine.hasNext());
    Assert.assertEquals("Line 1", rawStringPerLine.next());

    Assert.assertTrue(rawStringPerLine.hasNext());
    Assert.assertEquals("Line 2", rawStringPerLine.next());

    Assert.assertFalse(rawStringPerLine.hasNext()); // No more lines
  }
}
