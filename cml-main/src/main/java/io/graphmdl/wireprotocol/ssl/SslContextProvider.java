/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.graphmdl.wireprotocol.ssl;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.graphmdl.PostgresWireProtocolConfig;
import io.graphmdl.spi.CmlException;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.graphmdl.wireprotocol.PostgresWireProtocolErrorCode.WRONG_SSL_CONFIGURATION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class SslContextProvider
{
    private static final String TLS_VERSION = "TLSv1.2";
    private static final char[] KEYSTORE_PASSWORD = "cannerflowpassword".toCharArray();
    private static final Logger LOG = Logger.get(SslContextProvider.class);

    private SslContext sslContext;

    private final TlsDataProvider tlsDataProvider;
    private final PostgresWireProtocolConfig config;

    @Inject
    public SslContextProvider(PostgresWireProtocolConfig config, TlsDataProvider tlsDataProvider)
    {
        this.config = requireNonNull(config, "config is null");
        this.tlsDataProvider = requireNonNull(tlsDataProvider, "tlsDataProvider is null");
    }

    public SslContext getServerContext()
    {
        if (!config.isSslEnable()) {
            return null;
        }

        if (sslContext == null) {
            sslContext = serverContext();
        }

        return sslContext;
    }

    private SslContext serverContext()
    {
        try {
            KeyStore keyStore = loadKeyStore();
            KeyManager[] keyManagers = createKeyManagers(keyStore);

            Optional<KeyStore> trustStore = Optional.of(loadTrustStore());
            TrustManager[] trustManagers = trustStore
                    .map(SslContextProvider::createTrustManagers)
                    .orElseGet(() -> new TrustManager[0]);

            SSLContext sslContext = SSLContext.getInstance(TLS_VERSION);
            sslContext.init(keyManagers, trustManagers, null);

            X509Certificate[] keyStoreCertChain = getCertificateChain(keyStore);

            PrivateKey privateKey = getPrivateKey(keyStore);
            return SslContextBuilder
                    .forServer(privateKey, keyStoreCertChain)
                    .ciphers(List.of(sslContext.createSSLEngine().getEnabledCipherSuites()))
                    .applicationProtocolConfig(ApplicationProtocolConfig.DISABLED)
                    // set ClientAuth.OPTIONAL mean client doesn't need to be trusted by server.
                    .clientAuth(ClientAuth.OPTIONAL)
                    .trustManager(keyStoreCertChain)
                    .sessionCacheSize(0)
                    .sessionTimeout(0)
                    .startTls(false)
                    .sslProvider(SslProvider.JDK)
                    .build();
        }
        catch (Exception e) {
            LOG.error(e);
            throw new CmlException(WRONG_SSL_CONFIGURATION, "Failed to build SSL configuration: " + e.getMessage(), e);
        }
    }

    private KeyStore loadKeyStore()
            throws Exception
    {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, KEYSTORE_PASSWORD);
        Map<String, byte[]> tlsData = tlsDataProvider.getTlsData();
        CertificateFactory fact = CertificateFactory.getInstance("X.509");
        byte[] certBytes = tlsData.get("tls.crt");
        byte[] keyBytes = tlsData.get("tls.key");
        Collection<? extends Certificate> certs = fact.generateCertificates(new ByteArrayInputStream(certBytes));

        PEMParser parser = new PEMParser(new InputStreamReader(new ByteArrayInputStream(keyBytes), UTF_8));
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
        Object obj = parser.readObject();
        Certificate[] chain = certs.toArray(new Certificate[] {});

        if (obj instanceof PEMKeyPair) {
            // for k8s secret
            KeyPair kp = converter.getKeyPair((PEMKeyPair) obj);
            keyStore.setKeyEntry("default-tls", kp.getPrivate(), KEYSTORE_PASSWORD, chain);
        }
        else if (obj instanceof PrivateKeyInfo) {
            // for unit testing
            PrivateKey key = converter.getPrivateKey((PrivateKeyInfo) obj);
            keyStore.setKeyEntry("default-tls", key, KEYSTORE_PASSWORD, chain);
        }

        return keyStore;
    }

    private KeyStore loadTrustStore()
            throws Exception
    {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, KEYSTORE_PASSWORD);
        return keyStore;
    }

    private KeyManager[] createKeyManagers(KeyStore keyStore)
            throws Exception
    {
        KeyManagerFactory keyFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyFactory.init(keyStore, KEYSTORE_PASSWORD);
        return keyFactory.getKeyManagers();
    }

    private static TrustManager[] createTrustManagers(KeyStore keyStore)
    {
        TrustManagerFactory trustFactory;
        try {
            trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustFactory.init(keyStore);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return trustFactory.getTrustManagers();
    }

    private static X509Certificate[] getCertificateChain(KeyStore keyStore)
            throws KeyStoreException
    {
        ArrayList<X509Certificate> certs = new ArrayList<>();
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                Certificate[] certificateChain = keyStore.getCertificateChain(alias);
                if (certificateChain != null) {
                    for (Certificate certificate : certificateChain) {
                        certs.add((X509Certificate) certificate);
                    }
                }
            }
        }
        return certs.toArray(new X509Certificate[0]);
    }

    private static PrivateKey getPrivateKey(KeyStore keyStore)
            throws KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException
    {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                Key key = keyStore.getKey(alias, KEYSTORE_PASSWORD);
                if (key instanceof PrivateKey) {
                    return (PrivateKey) key;
                }
            }
        }
        throw new KeyStoreException("No fitting private key found in keyStore");
    }
}
