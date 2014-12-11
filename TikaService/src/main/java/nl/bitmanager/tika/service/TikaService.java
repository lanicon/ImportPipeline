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
    	System.out.println("starting");
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
            System.out.println ("Shutdown-2");
            os.close();
            System.out.println ("Shutdown-3");
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

    // ChunkedByteArrayOutputStream

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
                // if (compress) stream = new DeflaterOutputStream (buffer, new Deflater(), 16*1024);
                if (compress)
                    stream = new GZIPOutputStream(buffer, 16 * 1024);
                //

                final org.apache.tika.metadata.Metadata metadata = new org.apache.tika.metadata.Metadata();
                final AutoDetectParser parser = new AutoDetectParser(new DefaultDetector());
                final ParseContext context = new ParseContext();
                context.set(Parser.class, parser);
                input = TikaInputStream.get(url, metadata);
                ContentHandler contentHandler = getTransformerHandler(stream, "html", "utf-8", true);
                // System.out.println("outputstrem=" + outputStream);
                // System.out.println("input=" + input);
                // System.out.println("contentHandler=" + contentHandler);
                // System.out.println("metadata=" + metadata);
                // System.out.println("context=" + context);
                // System.out.println("outputstrem=" + outputStream);
                // System.out.println("outputstrem=" + outputStream);

                parser.parse(input, contentHandler, metadata, context);
                Headers h = t.getResponseHeaders();
                // if (compress) {
                // ((DeflaterOutputStream)stream).finish();
                // stream.flush();
                // h.add("Content-Encoding", "deflate");
                // }
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
}//
// /**
// * Helper for the importer:
// * It opens an http service to do tika conversions
// *
// */
//
// @Path("/")
// @Component
// @Scope("request")
// public class TikaService {
// private final String UTF8 = "UTF-8";
// private final Log logger = LogFactory.getLog(TikaService.class);
//
// // @GET
// // @Produces({ MediaType.TEXT_PLAIN})
// // @Path("/test/{pathkey}/{resourceid: .*}")
// // public Response test (
// // @javax.ws.rs.core.Context javax.ws.rs.core.UriInfo uriInfo,
// // @PathParam("pathkey") final String pathkey,
// // @PathParam("resourceid") final String resourceId,
// // @Context HttpHeaders httpHeaders) throws Exception {
// // try {
// // //ByteOutputStream outs = new ByteOutputStream();
// // GenericEntity. y = null;
// // return Response.ok(new GenericEntity<Stream>(new StringReader(" en bah"))).build();
// // //javax.ws.rs.core.StreamingOutput.
// // //throw new InternalServerErrorException ("boe");
// // } catch (Exception mex) {
// // throw new WebApplicationException(mex, Response.Status.NOT_FOUND);
// // }
// //
// //// };
// // }
//
// /**
// * Converts a file into html using Tika
// */
// @GET
// @Produces({ MediaType.TEXT_HTML})
// @Path("/html/{pathkey}/{resourceid: .*}")
// public StreamingOutput getHtml (
// @javax.ws.rs.core.Context javax.ws.rs.core.UriInfo uriInfo,
// @PathParam("pathkey") final String pathkey,
// @PathParam("resourceid") final String resourceId,
// @Context HttpHeaders httpHeaders) throws Exception {
// //System.out.println("pathkey=" + pathkey);
// //System.out.println("resourceId=" + resourceId);
//
// // get the resource segment, this may have query params
// // we are ok with it as long as we can get something at that location
// String[] segments = uriInfo.getRequestUri().toString()
// .split("/html/" + pathkey + "/");
// final String filename = URLDecoder.decode(segments[segments.length - 1], UTF8);
// //logger.info("resource :" + segments[segments.length - 1]);
//
// final org.apache.tika.metadata.Metadata metadata = new org.apache.tika.metadata.Metadata();
// final AutoDetectParser parser = new AutoDetectParser(new DefaultDetector());
// final ParseContext context = new ParseContext();
// context.set(Parser.class, parser);
//
// URL url = null;
// try {
// String path = getFilePath(pathkey);
// String filepath = path + filename;
// //System.out.println("fp=" + filepath);
// File file = new File(filepath);
// if (file.isFile()) {
// //System.out.println("isfile");
// url = file.toURI().toURL();
// } else {
// //System.out.println("isnotfile");
// url = new URL(filepath);
// }
// } catch (MalformedURLException mex) {
// throw new WebApplicationException(mex, Response.Status.NOT_FOUND);
// }
//
// // System.out.println("");
// System.out.println("Processing " + url);
// final URL Url = url;
// return new StreamingOutput() {
// public void write(OutputStream outputStream) throws IOException, WebApplicationException {
//
// InputStream input = TikaInputStream.get(Url, metadata);
// try {
// ContentHandler contentHandler = getTransformerHandler(outputStream, "html", UTF8, true);
// // System.out.println("outputstrem=" + outputStream);
// // System.out.println("input=" + input);
// // System.out.println("contentHandler=" + contentHandler);
// // System.out.println("metadata=" + metadata);
// // System.out.println("context=" + context);
// // System.out.println("outputstrem=" + outputStream);
// // System.out.println("outputstrem=" + outputStream);
//
// parser.parse(input, contentHandler, metadata, context);
// }
// catch (Exception err) {
// System.err.println();
// System.err.println("Error while processing " + Url);
// err.printStackTrace();
// //throw new WebApplicationException(err.getMessage(), err, Response.Status.INTERNAL_SERVER_ERROR);
// throw new WebApplicationException(err, Response.Status.INTERNAL_SERVER_ERROR);
// }
// finally {
// input.close();
// }
// }
// };
// }
//
//
//
//
// /**
// * Simple ping request that responds with pong
// */
// @GET
// @Produces({ MediaType.TEXT_PLAIN})
// @Path("/ping")
// public StreamingOutput ping (
// @javax.ws.rs.core.Context javax.ws.rs.core.UriInfo uriInfo,
// @Context HttpHeaders httpHeaders) throws Exception {
// return new StreamingOutput() {
// public void write(OutputStream outputStream) throws IOException, WebApplicationException {
//
// outputStream.write((byte)'p');
// outputStream.write((byte)'o');
// outputStream.write((byte)'n');
// outputStream.write((byte)'g');
// }
// };
// }
//
//
// /**
// * Shutdown this service
// */
// @GET
// @Produces({ MediaType.TEXT_PLAIN})
// @Path("/shutdown")
// public StreamingOutput shutdown (
// @javax.ws.rs.core.Context javax.ws.rs.core.UriInfo uriInfo,
// @Context HttpHeaders httpHeaders) throws Exception {
// return new StreamingOutput() {
// public void write(OutputStream outputStream) throws IOException, WebApplicationException {
//
// outputStream.write((byte)'s');
// outputStream.write((byte)'h');
// outputStream.write((byte)'u');
// outputStream.write((byte)'t');
// outputStream.write((byte)'d');
// outputStream.write((byte)'o');
// outputStream.write((byte)'w');
// outputStream.write((byte)'n');
// outputStream.flush();
// System.exit(0);
// }
// };
// }
//
//
//
//
//
// /**
// * Returns a transformer handler that serializes incoming SAX events
// * to XHTML or HTML (depending the given method) using the given output
// * encoding.
// *
// * @see <a href="https://issues.apache.org/jira/browse/TIKA-277">TIKA-277</a>
// * @param output output stream
// * @param method "xml" or "html"
// * @param encoding output encoding,
// * or <code>null</code> for the platform default
// * @return {@link System#out} transformer handler
// * @throws TransformerConfigurationException
// * if the transformer can not be created
// */
// private static TransformerHandler getTransformerHandler(OutputStream output, String method, String encoding, boolean prettyPrint)
// throws TransformerConfigurationException {
// SAXTransformerFactory factory = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
// TransformerHandler handler = factory.newTransformerHandler();
// handler.getTransformer().setOutputProperty(OutputKeys.METHOD, method);
// handler.getTransformer().setOutputProperty(OutputKeys.INDENT, prettyPrint ? "yes" : "no");
// if (encoding != null) {
// handler.getTransformer().setOutputProperty( OutputKeys.ENCODING, encoding);
// }
// handler.setResult(new StreamResult(output));
// return handler;
// }
//
//
//
// // private Detector createDetector(HttpHeaders httpHeaders) throws IOException,
// // TikaException {
// //// final javax.ws.rs.core.MediaType mediaType = httpHeaders.getMediaType();
// //// if (mediaType == null
// //// || mediaType
// //// .equals(javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM_TYPE))
// // return (new TikaConfig()).getMimeRepository();
// //// else
// //// return new Detector() {
// ////
// //// public org.apache.tika.mime.MediaType detect(
// //// InputStream inputStream,
// //// org.apache.tika.metadata.Metadata metadata)
// //// throws IOException {
// //// return org.apache.tika.mime.MediaType.parse(mediaType
// //// .toString());
// //// }
// //// };
// // }
//
// // /**
// // * Returns a output writer with the given encoding.
// // *
// // * @see <a
// // * href="https://issues.apache.org/jira/browse/TIKA-277">TIKA-277</a>
// // * @param output
// // * output stream
// // * @param encoding
// // * output encoding, or <code>null</code> for the platform default
// // * @return output writer
// // * @throws UnsupportedEncodingException
// // * if the given encoding is not supported
// // */
// // private static Writer getOutputWriter(OutputStream output, String encoding)
// // throws UnsupportedEncodingException {
// // if (encoding != null) {
// // return new OutputStreamWriter(output, encoding);
// // } else if (System.getProperty("os.name").toLowerCase()
// // .startsWith("mac os x")) {
// // // TIKA-324: Override the default encoding on Mac OS X
// // return new OutputStreamWriter(output, "UTF-8");
// // } else {
// // return new OutputStreamWriter(output);
// // }
// // }
//
// /**
// * Returns a URL for pathkey from JNDI. Used in calls that processes
// * network-accessible files where you don't want to expose the absolute path
// * Ensure pathkey is available in JNDI
// *
// * @return filepath
// */
// private String getFilePath(String pathkey) {
// if (pathkey==null || pathkey.equals("filesystem")) return "";
// logger.info("Getting path for "+pathkey);
// String path = "";
// try {
// javax.naming.Context initCtx = new InitialContext();
// path = (String) initCtx.lookup("java:comp/env/"+pathkey);
// } catch (NamingException e) {
// e.printStackTrace();
// }
// logger.info("returning: " + path);
// return path;
// }
//
// }
