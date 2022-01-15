package org.opendedup.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import javax.xml.bind.DatatypeConverter;

import com.google.common.io.BaseEncoding;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class GetKeys {
    private static final String DELIMITER = ";;";

    public static Options buildOptions() {
        Options options = new Options();
        options.addOption("j", true,
                "REQUIRED - Jar path \n e.g. \'C:\\Program Files\\OmniBack\\bin\\tools\\converter-tool.jar\'");
        options.addOption("a", true, "REQUIRED - api string");
        options.addOption("s", false, "get server certs");
        options.addOption("h", false, "display available options");
        return options;
    }

    public static void main(String[] args)
            throws ParseException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        CommandLineParser parser = new PosixParser();
        Options options = buildOptions();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h") || !cmd.hasOption("j") || !cmd.hasOption("a")) {
            printHelp(options);
            System.exit(1);
        }
        if (cmd.hasOption("s")) {
            try {
                printServerKey(cmd.getOptionValue("j"), cmd.getOptionValue("a"));
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(2);
            }
        }
        if (cmd.hasOption("c")) {
            try {
                byte[] kb = BaseEncoding.base64().decode(cmd.getOptionValue("c"));
                CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
                X509Certificate cer = (X509Certificate) certFactory.generateCertificate(new ByteArrayInputStream(kb));
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(3);
            }
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("checkkey  ",
                options);
    }

    private static void printServerKey(String cp, String apiStr) throws Exception {
        java.util.List<String> classInfo = Arrays.asList(apiStr.split(DELIMITER));
        String loadclass = classInfo.get(0);
        String key_method = classInfo.get(1);
        String path_method = classInfo.get(2);
        URL[] classLoaderUrls = new URL[] { new URL("file:" + cp) };
        // Create a new URLClassLoader
        URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls);
        // Load the target class
        Class<?> beanClass = urlClassLoader.loadClass(loadclass);
        // Create a new instance from the loaded class
        Constructor<?> constructor = beanClass.getConstructor();
        Object beanObj = constructor.newInstance();
        // Getting a method from the loaded class and invoke it
        Method method = beanClass.getMethod(path_method);
        String path = (String) method.invoke(beanObj);
        java.util.List<String> prodInfo = Arrays.asList(path.split(DELIMITER));
        String certChainFilePath = prodInfo.get(0);
        // Getting a method from the loaded class and invoke it
        Method method2 = beanClass.getMethod(key_method);
        PrivateKey pvtKey = (PrivateKey) method2.invoke(beanObj);
        urlClassLoader.close();
        writeKey(System.out, pvtKey);
        X509Certificate serverCertChain = getX509Certificate(certChainFilePath);
        writeCertificate(System.out, serverCertChain);
    }

    private static X509Certificate getX509Certificate(String certPath) throws Exception {
        FileInputStream is = null;
        try {
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
            is = new FileInputStream(certPath);
            X509Certificate cer = (X509Certificate) certFactory.generateCertificate(is);
            return cer;
        } catch (CertificateException e) {
            throw new Exception(e);
        } catch (FileNotFoundException e) {
            throw new Exception(e);
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    static void writeCertificate(OutputStream out, X509Certificate crt) throws Exception {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write("-----BEGIN CERTIFICATE-----\r\n".getBytes());
        writeBufferBase64(baos, crt.getEncoded());
        baos.write("-----END CERTIFICATE-----\r\n".getBytes());
        out.write(baos.toByteArray());
        out.flush();
        out.close();
        System.out.println(baos.toString());
    }

    static void writeKey(OutputStream out, PrivateKey pk) throws Exception {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String fmt = pk.getFormat();
        if ("PKCS#8".equals(fmt)) {
            baos.write("-----BEGIN PRIVATE KEY-----\r\n".getBytes());
            writeBufferBase64(baos, pk.getEncoded());
            baos.write("-----END PRIVATE KEY-----\r\n".getBytes());
        } else if ("PKCS#1".equals(fmt)) {
            baos.write("-----BEGIN RSA PRIVATE KEY-----\r\n".getBytes());
            writeBufferBase64(baos, pk.getEncoded());
            baos.write("-----END RSA PRIVATE KEY-----\r\n".getBytes());
        }
        out.write(baos.toByteArray());
        out.flush();
        out.close();
        System.out.println(baos.toString());
    }

    static void writeBufferBase64(OutputStream out, byte[] bufIn) throws IOException {
        final byte[] buf = DatatypeConverter.printBase64Binary(bufIn).getBytes();
        final int BLOCK_SIZE = 64;
        for (int i = 0; i < buf.length; i += BLOCK_SIZE) {
            out.write(buf, i, Math.min(BLOCK_SIZE, buf.length - i));
            out.write('\r');
            out.write('\n');
        }
    }

}
