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

package io.cdap.plugin.http.source.common.pagination;

import com.google.common.base.Strings;
import org.python.core.PyException;
import org.python.core.PyObject;
import org.python.core.PySyntaxError;
import org.python.util.PythonInterpreter;

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

/**
 * Executes python method 'get_next_page_url' using Jython library
 */
public class JythonPythonExecutor implements Closeable {
  private static final String NEXT_PAGE_FUNCTION_NAME = "get_next_page_url";

  private final String script;
  private PythonInterpreter interpreter;
  private PythonAccessor pythonAccessor;

  public JythonPythonExecutor(String script) {
    this.script = script;
  }

  public void initialize() {
    interpreter = new PythonInterpreter();

    try {
      interpreter.exec(script);
    } catch (PySyntaxError e) {
      if (Strings.isNullOrEmpty(e.getMessage())) {
        throw new RuntimeException(
          "Unknown syntax error occurred while interpreting python code. Please make sure syntax is correct.", e);
      }
      throw new RuntimeException(
        String.format("Syntax error occurred while interpreting python code: '%s'", e.getMessage()), e);
    }

    PyObject tmpFunction = interpreter.get(NEXT_PAGE_FUNCTION_NAME);

    if (tmpFunction == null) {
      throw new IllegalStateException(
        String.format("Function '%s' is not found in Python script", NEXT_PAGE_FUNCTION_NAME));
    }

    pythonAccessor = (PythonAccessor) tmpFunction.__tojava__(PythonAccessor.class);
  }

  public String getNextPageUrl(String url, String page, Map<String, String> headers) {
    try {
      return pythonAccessor.get_next_page_url(url, page, headers);
    } catch (PyException e) {
      // Put stack trace as the exception message, because otherwise the information from PyException is lost.
      // PyException only exposes the actual cause (Python stack trace) if printStackTrace() is called on it.
      throw new RuntimeException(String.format("Exception while running '%s'.\n%s",
                                               NEXT_PAGE_FUNCTION_NAME, getStackTrace(e)));
    }
  }

  public void close() {
    if (interpreter != null) {
      interpreter.cleanup();
    }
  }

  private static String getStackTrace(Throwable throwable) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    throwable.printStackTrace(pw);
    return sw.toString();
  }

  private interface PythonAccessor {
    String get_next_page_url(String url, String page, Map<String, String> headers);
  }
}
