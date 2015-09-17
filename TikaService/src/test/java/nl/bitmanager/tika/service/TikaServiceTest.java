/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package nl.bitmanager.tika.service;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import com.sun.net.httpserver.Headers;

public class TikaServiceTest {

    @Test
    public void test() throws Exception {
        ChunkedByteArrayOutputStream buffer = new ChunkedByteArrayOutputStream(16 * 1024);
        URL url = null;
        File file = new File("c:/tmp/20140201 125014 Ardennen Troisvierges - Cherain (D).jpg");
        url = file.toURI().toURL();
        System.out.println("URL=" + url);

        OutputStream stream = buffer;

        final org.apache.tika.metadata.Metadata metadata = new org.apache.tika.metadata.Metadata();
        final AutoDetectParser parser = new AutoDetectParser(new DefaultDetector());
        final ParseContext context = new ParseContext();
        context.set(Parser.class, parser);
        TikaInputStream input = TikaInputStream.get(url, metadata);
        ContentHandler contentHandler = getTransformerHandler(stream, "html", "utf-8", true);

        parser.parse(input, contentHandler, metadata, context);
        // fix for TIKA-596: if a parser doesn't generate
        // XHTML output, the lack of an output document prevents
        // metadata from being output: this fixes that
        contentHandler.endDocument();
//        if (contentHandler instanceof NoDocumentMetHandler){
//            NoDocumentMetHandler metHandler = (NoDocumentMetHandler)contentHandler;
//            if(!metHandler.metOutput()){
//                metHandler.endDocument();
//            }
//        }

        OutputStream os = new FileOutputStream("out.html");
        buffer.writeTo(os);
        os.close();
    }
    
    private static TransformerHandler getTransformerHandler(OutputStream output, String method, String encoding, boolean prettyPrint)
            throws TransformerConfigurationException {
        SAXTransformerFactory factory = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
        TransformerHandler handler = factory.newTransformerHandler();
        handler.getTransformer().setOutputProperty(OutputKeys.METHOD, method);
        handler.getTransformer().setOutputProperty(OutputKeys.INDENT, prettyPrint ? "yes" : "no");
        if (encoding != null) {
            handler.getTransformer().setOutputProperty(OutputKeys.ENCODING, encoding);
        }
        handler.setResult(new StreamResult(output));
        return handler;
    }


}
