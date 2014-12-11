/*
 * This file is licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.bitmanager.tika.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
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
import org.xml.sax.ContentHandler;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class TikaService {
    public static Charset utf8 = Charset.forName("UTF-8");// .encode(myString)

    public static void main(String[] args) throws Exception {
    	int N = args.length;
        if ((args.length %2) != 0) {
            System.err.println("Usage: TikaService [/ip ip] [/port port]");
            System.exit(12);
        }
       
        int port=8080;
        String ip = null;
        
        for (int i=0; i<N; i+=2) {
        	String key = args[i].toLowerCase();
        	if ("/port".equals (key)) {
        		port = Integer.parseInt(args[i+1]);
        		continue;
        	}
        	if ("/ip".equals (key)) {
        		ip = args[i+1];
        		continue;
        	}
        }
        
        
        System.out.println ("Starting server from ip=" + ip + ", port=" + port + ".");
        InetSocketAddress addr = ip==null ? new InetSocketAddress (port) : new InetSocketAddress (ip, port);
        HttpServer server = HttpServer.create(addr, 0);
        server.createContext("/shutdown", new ShutdownHandler(server));
        server.createContext("/ping", new PingHandler());
        server.createContext("/convert", new ConvertHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println ("Started");
    }

    static class ShutdownHandler implements HttpHandler {
        private final HttpServer server;

        public ShutdownHandler(HttpServer server) {
            this.server = server;
        }

        public void handle(HttpExchange t) throws IOException {
            // add the required response header for a PDF file
            Headers h = t.getResponseHeaders();
            h.add("Content-Type", "text/plain");

            System.out.println ("Shutdown...");
            String response = "Shutting down...";
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
            t.close();

            
            System.out.println ("Stopping");
            server.stop(0);
            System.out.println ("Sleeping");
            try {
				Thread.sleep(100);
			} catch (InterruptedException e) {}
            System.out.println ("exit");
            System.exit(0);
        }
    }


    static class PingHandler implements HttpHandler {
        static byte[] resp = new byte[] { 'p', 'o', 'n', 'g' };

        public void handle(HttpExchange t) throws IOException {
            System.out.println ("Ping...");
            Headers h = t.getResponseHeaders();
            h.add("Content-Type", "text/plain; charset=utf-8");

            t.sendResponseHeaders(200, resp.length);
            OutputStream os = t.getResponseBody();
            os.write(resp);
            os.close();
        }
    }

    static class ConvertHandler implements HttpHandler {

        @SuppressWarnings("unchecked")
        private void parseQuery(String query, Map<String, Object> parameters) throws UnsupportedEncodingException {
            String encoding = System.getProperty("file.encoding");

            if (query != null) {
                String pairs[] = query.split("[&]");

                for (String pair : pairs) {
                    String param[] = pair.split("[=]");

                    String key = null;
                    String value = null;
                    if (param.length > 0) {
                        key = URLDecoder.decode(param[0], encoding);
                    }

                    if (param.length > 1) {
                        value = URLDecoder.decode(param[1], encoding);
                    }

                    if (parameters.containsKey(key)) {
                        Object obj = parameters.get(key);
                        if (obj instanceof List<?>) {
                            List<String> values = (List<String>) obj;
                            values.add(value);
                        } else if (obj instanceof String) {
                            List<String> values = new ArrayList<String>();
                            values.add((String) obj);
                            values.add(value);
                            parameters.put(key, values);
                        }
                    } else {
                        parameters.put(key, value);
                    }
                }
            }
        }

        public void handle(HttpExchange t) throws IOException {
            InputStream input = null;
            String fileParm = null;
            ChunkedByteArrayOutputStream buffer = null;
            try {
                URI uri = t.getRequestURI();
                Map<String, Object> parameters = new HashMap<String, Object>();
                parseQuery(uri.getQuery(), parameters);
                fileParm = (String) parameters.get("file");
                if (fileParm == null)
                    throw new RuntimeException("Missing file-parameter.");

                boolean compress = "true".equals(parameters.get("compress"));
                buffer = new ChunkedByteArrayOutputStream(16 * 1024);
                buffer.setMaxSize((String)parameters.get("maxsize"));
                //System.out.println("NOT isfileparm='" + fileParm + "'");
                URL url = null;
                File file = new File(fileParm);
                if (file.isFile()) {
                    //System.out.println("isfile");
                    url = file.toURI().toURL();
                } else {
                    //System.out.println("NOT isfile");
                    url = new URL(fileParm);
                }
                System.out.println("URL=" + url);

                OutputStream stream = buffer;
                if (compress)
                    stream = new GZIPOutputStream(buffer, 16 * 1024);

                final org.apache.tika.metadata.Metadata metadata = new org.apache.tika.metadata.Metadata();
                final AutoDetectParser parser = new AutoDetectParser(new DefaultDetector());
                final ParseContext context = new ParseContext();
                context.set(Parser.class, parser);
                input = TikaInputStream.get(url, metadata);
                ContentHandler contentHandler = getTransformerHandler(stream, "html", "utf-8", true);

                parser.parse(input, contentHandler, metadata, context);
                Headers h = t.getResponseHeaders();
                if (compress) {
                    ((GZIPOutputStream) stream).finish();
                    stream.flush();
                    h.add("Content-Encoding", "gzip");
                }
                //System.out.println("Sending " + buffer.getLength() + " bytes");

                h.add("Content-Type", "text/html; charset=utf-8;");
                t.sendResponseHeaders(200, buffer.getLength());
                OutputStream os = t.getResponseBody();
                buffer.writeTo(os);
                os.close();

            } catch (Exception e) {
                buffer = null;
                if (input != null)
                    input.close();
                System.gc();
                writeException(t, e, fileParm);

            } finally {
                if (input != null)
                    input.close();
                t.close();
            }
        }

        private void writeException(HttpExchange t, Exception e, String fileParm) throws IOException {
            StringBuilder sb = new StringBuilder();
            sb.append("ERROR: ");
            sb.append(e.getMessage());
            sb.append("\r\nFile: ");
            sb.append(fileParm);
            String msg = sb.toString();
            System.out.println();
            System.out.println(msg);
            e.printStackTrace();

            byte[] b = msg.getBytes("utf-8");
            Headers h = t.getResponseHeaders();
            h.add("Content-Type", "text/plain; charset=utf-8");
            h.clear();
            t.sendResponseHeaders(500, b.length);
            OutputStream os = t.getResponseBody();
            os.write(b);
            os.close();
        }

        /**
         * Returns a transformer handler that serializes incoming SAX events to XHTML or HTML (depending the given method) using the given output encoding.
         * 
         * @see <a href="https://issues.apache.org/jira/browse/TIKA-277">TIKA-277</a>
         * @param output
         *            output stream
         * @param method
         *            "xml" or "html"
         * @param encoding
         *            output encoding, or <code>null</code> for the platform default
         * @return {@link System#out} transformer handler
         * @throws TransformerConfigurationException
         *             if the transformer can not be created
         */
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
}
