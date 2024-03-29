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

import com.google.common.base.Strings;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * Class which creates an SSLConnectionSocketFactory.
 */
public class SSLConnectionSocketFactoryCreator {
  private static final Logger LOG = LoggerFactory.getLogger(SSLConnectionSocketFactoryCreator.class);

  private final BaseHttpSourceConfig config;

  public SSLConnectionSocketFactoryCreator(BaseHttpSourceConfig config) {
    this.config = config;
  }

  public SSLConnectionSocketFactory create() {
    String cipherSuitesString = config.getCipherSuites();
    // Usually colons are used as separators, but commas or spaces are also acceptable.
    String[] cipherSuites = (cipherSuitesString == null) ? null : cipherSuitesString.split("[:, ]");

    try {
      SSLContext sslContext = SSLContext.getInstance("TLS"); // "TLS" means rely system properties
      sslContext.init(getKeyManagers(), getTrustManagers(), null);

      return new SSLConnectionSocketFactory(sslContext, config.getTransportProtocolsList().toArray(new String[0]),
                                            cipherSuites, SSLConnectionSocketFactory.getDefaultHostnameVerifier());
    } catch (KeyManagementException | CertificateException | NoSuchAlgorithmException | KeyStoreException
      | IOException | UnrecoverableKeyException e) {
      throw new IllegalStateException("Failed to create an SSL connection factory.", e);
    }
  }

  private KeyManager[] getKeyManagers() throws CertificateException, NoSuchAlgorithmException,
    KeyStoreException, IOException, UnrecoverableKeyException {

    KeyStore keystore = loadKeystore(config.getKeystoreFile(), config.getKeystoreType().name(),
                                     config.getKeystorePassword());

    String keyStorePassword = config.getKeystorePassword();

    // we have to manually fall back to default keystore. SSLContext won't provide such a functionality.
    if (keystore == null) {
      String keyStore = System.getProperty("javax.net.ssl.keyStore");
      String keyStoreType = System.getProperty("javax.net.ssl.keyStoreType", KeyStore.getDefaultType());
      keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword", "");

      keystore = loadKeystore(keyStore, keyStoreType, keyStorePassword);
    }

    String keystoreAlgorithm =
      (Strings.isNullOrEmpty(config.getKeystoreKeyAlgorithm())) ? KeyManagerFactory.getDefaultAlgorithm()
        : config.getKeystoreKeyAlgorithm();
    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(keystoreAlgorithm);
    char[] passwordArr = (keyStorePassword == null) ? null : keyStorePassword.toCharArray();
    keyManagerFactory.init(keystore, passwordArr);
    return keyManagerFactory.getKeyManagers();
  }

  private TrustManager[] getTrustManagers()
    throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {

    if (!config.getVerifyHttps()) {
      return new TrustManager[] { new TrustAllTrustManager() };
    }

    KeyStore trustStore = loadKeystore(config.getTrustStoreFile(), config.getTrustStoreType().name(),
                                       config.getTrustStorePassword());
    TrustManager[] trustManagers = null;
    if (trustStore != null) {
      String trustStoreAlgorithm =
        (Strings.isNullOrEmpty(config.getTrustStoreKeyAlgorithm())) ? TrustManagerFactory.getDefaultAlgorithm()
          : config.getTrustStoreKeyAlgorithm();
      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(trustStoreAlgorithm);
      trustManagerFactory.init(trustStore);
      trustManagers = trustManagerFactory.getTrustManagers();
    }
    return trustManagers;
  }

  private static KeyStore loadKeystore(String keystoreFile, String type, String password)
    throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException {

    KeyStore keystore = null;
    if (keystoreFile != null) {
      keystore = KeyStore.getInstance(type);
      char[] passwordArr = (password == null) ? null : password.toCharArray();
      try (InputStream is = Files.newInputStream(Paths.get(keystoreFile))) {
        keystore.load(is, passwordArr);
      }
    }
    return keystore;
  }
}
