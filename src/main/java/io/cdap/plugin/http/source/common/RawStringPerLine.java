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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Class that reads the raw string from the HTTP response and returns it line by line.
 */
public class RawStringPerLine implements Closeable, Iterator<String> {
    protected final HttpResponse httpResponse;
    private BufferedReader bufferedReader;
    private boolean isLineRead;
    private String lastLine;

    public RawStringPerLine(HttpResponse httpResponse) {
        this.httpResponse = httpResponse;
    }

    private BufferedReader getBufferedReader() throws IOException {
        if (bufferedReader == null) {
            this.bufferedReader = new BufferedReader(new InputStreamReader(httpResponse.getInputStream()));
        }
        return bufferedReader;
    }

    @Override
    public void close() throws IOException {
        if (bufferedReader != null) {
            bufferedReader.close();
        }
    }

    @Override
    public boolean hasNext() {
        try {
            if (!isLineRead) {
                lastLine = this.getBufferedReader().readLine();
            }
            isLineRead = true;
            return lastLine != null;
        } catch (IOException e) { // we need to catch this, since hasNext() does not have "throws" in parent
            throw new RuntimeException("Failed to read line from http page buffer", e);
        }
    }

    @Override
    public String next() {
        if (!hasNext()) { // calling hasNext will also read the line;
            throw new NoSuchElementException();
        }
        isLineRead = false;
        return lastLine;
    }
}
