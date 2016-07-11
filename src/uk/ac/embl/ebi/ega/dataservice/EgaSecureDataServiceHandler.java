/*
 * Copyright 2015 EMBL-EBI.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This class provides responses to REST URLs
 * This service will run ONLY inside the EGA Vault and is not available anywhere else
 * For this reason it uses plain http and no user account information
 *
 * URL Prefix for his server is: /ega/rest/data/v2
 *
 * Resources are:
 *
 * /datasets                                                (Test 1) all datasets
 * /datasets/{datasetid}/files                              (Test 2) all files in {datasetid}
 * /datasets/{datasetid}/users                              (Test 15) all users authorized for {datasetid}
 * /datasets/{datasetid}/users/{email}                      (Test 16) is {email} authorized for {datasetid}?
 *
 * /files/{file_id}                                         (Test 3) (relative path for a file_stable_id? File Info...)
 * 
 * /users/{email}/requests                                  (Test 8) all request tickets for user {email}
 * /users/{email}/requests/request/{label}/datasets         (Test 10) all datasets in request {request} for user {email}
 * /users/{email}/requests/request/{label}/tickets          (Test 9) all tickets in request {request} for user {email}
 * /users/{email}/requests/request/{label}/localize?ip={ip} (Test 12 TODO) localize tickets in request {request} for user {email}
 * /users/{email}/requests/ticket/{ticket}                  (Test 11) ticket info for {ticket} for user {email}
 * /users/{email}/datasets                                  (Test 4) all datasets for user {email}
 * /users/{email}/datasets/{datasetid}/files                (Test 5) all files in dataset {dataset} for user {email}
 * /users/{email}/datasets/dac/{dac}                        (Test 17) all datasets for user {email}, by DAC
 *
 * /tickets/{ticket}                                        (Test 11.5) details of one ticket
 *
 * /userposts/{email}/requests/download/file/{fileid} [POST]        (Test 7)
 * /userposts/{email}/requests/download/dataset/{datasetid} [POST]  (Test 6)
 *
 * /userdeletes/{email}/requests/delete/request/{label} [DELETE]    (Test 14)
 * /userdeletes/{email}/requests/delete/ticket/{ticket} [DELETE]    (Test 13)
 *
 * /stats/load                                              Server Load (total server CPU 0-100)
 *
 */

package uk.ac.embl.ebi.ega.dataservice;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import io.netty.handler.codec.http.HttpResponseStatus;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.util.CharsetUtil;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.URLDecoder;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import uk.ac.embl.ebi.ega.dataservice.endpoints.Service;
import uk.ac.embl.ebi.ega.dataservice.utils.DatabaseExecutor;
import uk.ac.embl.ebi.ega.dataservice.utils.MyPipelineUtils;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;

/**
 *
 * This is unique/exclusive for each connection - place user interaction caches here
 */
public class EgaSecureDataServiceHandler extends SimpleChannelInboundHandler<FullHttpRequest> { // (1)

    public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
    public static final String HTTP_DATE_GMT_TIMEZONE = "GMT";
    public static final int HTTP_CACHE_SECONDS = 60;
    
    public static double load_ceiling = 100.0;
    
    // Handle session unique information
    private boolean SSL = false, active = true;
    private final DatabaseExecutor dbe;
    private final HashMap<String, Service> endpointMappings;
    
    private final EgaSecureDataService ref;

    public EgaSecureDataServiceHandler(boolean SSL, HashMap<String, Service> mappings, DatabaseExecutor dbe, EgaSecureDataService ref) throws NoSuchAlgorithmException {
        super();
        this.SSL = SSL;
        this.endpointMappings = mappings;
        this.dbe = dbe;
        
        this.ref = ref;
    }

    // *************************************************************************
    // *************************************************************************
    @Override
    public void messageReceived(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        if (ctx==null) return; if (request==null) return; // Don't even proceed in these cases!
        
        // Step 1: Get IP; may be contained in a Header
        String get = request.headers().get("Accept").toString(); // Response Type

        // Step 2: Check Request
        HttpResponseStatus checkURL = MyPipelineUtils.checkURL(request);
        if (checkURL != OK) {
            sendError(ctx, checkURL, get);
            return;
        }
        
        // Step 3: Active?
        if (!EgaSecureDataService.keepRunning) {
            sendError(ctx, SERVICE_UNAVAILABLE, get); // Service is shutting down
            return;
        }
        double load = EgaSecureDataService.getSystemCpuLoad();

        // Step 4: process the path (1) verify root and service (2) determine function & resource
        String path = MyPipelineUtils.sanitizedUserAction(request);
        ArrayList<String> id = new ArrayList<>();
        String function = MyPipelineUtils.processUserURL(path, id);
        
        // Step 5: Extract any parameters sent with request
        Map<String, String> parameters = MyPipelineUtils.getParameters(path);
                
        // Past "limiters"
        long time_ = System.currentTimeMillis();
        
        // Filter out Database Change requsts, route to separate endpoint (can hide /userposts from API))
        if (request.method() == POST) {
            // Move Form values into parameters, to easily pass it to endpoint
            Map<String,String> body = new HashMap<>();
            body.put("id", "");
            body.put("rekey", "");
            body.put("downloadType", "");
            body.put("descriptor", "");
            decodeRequestBody(request, "downloadrequest", body);
            parameters.putAll(body);
            SocketAddress remoteAddress = ctx.channel().remoteAddress();
            String clientIP = remoteAddress.toString();
            if (clientIP.startsWith("/")) clientIP = clientIP.substring(1);
            if (clientIP.contains(":")) clientIP = clientIP.substring(0, clientIP.indexOf(":"));
            parameters.put("ip", clientIP);
            function = "/userposts";
        } else if (request.method() == DELETE) {
            function = "/userdeletes";
            SocketAddress remoteAddress = ctx.channel().remoteAddress();
            String clientIP = remoteAddress.toString();
            if (clientIP.startsWith("/")) clientIP = clientIP.substring(1);
            if (clientIP.contains(":")) clientIP = clientIP.substring(0, clientIP.indexOf(":"));
            parameters.put("ip", clientIP);
        } else if (request.method() == GET && (function.equalsIgnoreCase("/userposts") || function.equalsIgnoreCase("/userdeletes")))
            function = "/users";

        // ---------------------------------------------------------------------
        // Map function to endpoint: individual request (downloads follow later)
        // ---------------------------------------------------------------------
        JSONObject json = null;
        if (this.endpointMappings.containsKey(function)) {
            json = this.endpointMappings.get(function).handle(id, parameters, request, this.dbe, this.ref);
        } else {
            sendError(ctx, BAD_REQUEST, get); // If the URL Function is incorrect...
            return;
        }
        
        // Step 3: Prepare a response - set content typt to the expected type
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK);
        StringBuilder buf = new StringBuilder();        
        response.headers().set(CONTENT_TYPE, "application/json");
        buf.append(json.toString());
        
        // Step 4: Result has been obtained. Build response and send to requestor
        ByteBuf buffer = Unpooled.copiedBuffer(buf, CharsetUtil.UTF_8);
        response.content().writeBytes(buffer);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        
        // Cleanup
        buffer.release();
    }
    // Decodes the body of an HTTP POST request, places them in HashMap 'values' (passed in)
    private void decodeRequestBody(FullHttpRequest request, String formname, Map<String, String> values) throws JSONException, IOException {
        HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(new DefaultHttpDataFactory(false), request);

        InterfaceHttpData bodyHttpData = decoder.getBodyHttpData(formname);
        JSONObject json = new JSONObject(((Attribute)bodyHttpData).getValue());
        
        int v = 0;
        Set<String> keySet = values.keySet();
        if (!keySet.isEmpty()) {
            Iterator<String> iter = keySet.iterator();
            
            while (iter.hasNext()) {
                String key = URLDecoder.decode(iter.next(), "UTF-8");
                String put = values.put(key, URLDecoder.decode(json.get(key).toString(), "UTF-8"));
                if (put!=null && put.length() > 0)
                    v++;
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        if (ctx.channel().isActive()) {
            sendError(ctx, INTERNAL_SERVER_ERROR);
        }
    }

    // JSON Version of error messages
    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
        sendError(ctx, status, "application/json");
    }
    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String get) {
        EgaSecureDataService.log(status.toString());
        try {
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status);
            JSONObject json = new JSONObject(); // Start out with common JSON Object
            json.put("header", responseHeader(status)); // Header Section of the response
            json.put("response", "null"); // ??
            
            StringBuilder buf = new StringBuilder();
            response.headers().set(CONTENT_TYPE, "application/json");
            buf.append(json.toString());
            
            ByteBuf buffer = Unpooled.copiedBuffer(buf, CharsetUtil.UTF_8);
            response.content().writeBytes(buffer);
            
            // Close the connection as soon as the error message is sent.
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        } catch (JSONException ex) {
            Logger.getLogger(EgaSecureDataServiceHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    // Generate JSON Header Section
    private JSONObject responseHeader(HttpResponseStatus status) throws JSONException {
        return responseHeader(status, "");
    }
    private JSONObject responseHeader(HttpResponseStatus status, String error) throws JSONException {
        JSONObject head = new JSONObject();
        
        head.put("apiVersion", "v2");
        head.put("code", String.valueOf(status.code()));
        head.put("service", "data");
        head.put("technicalMessage", "");                   // TODO (future)
        head.put("userMessage", status.reasonPhrase());
        head.put("errorCode", String.valueOf(status.code()));
        head.put("docLink", "http://www.ebi.ac.uk/ega");    // TODO (future)
        head.put("errorStack", error);                     // TODO ??
        
        return head;
    }
}
