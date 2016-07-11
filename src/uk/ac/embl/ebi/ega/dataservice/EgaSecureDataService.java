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

package uk.ac.embl.ebi.ega.dataservice;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.lang.management.ManagementFactory;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import uk.ac.embl.ebi.ega.dataservice.endpoints.DatasetService;
import uk.ac.embl.ebi.ega.dataservice.endpoints.FileService;
import uk.ac.embl.ebi.ega.dataservice.endpoints.Service;
import uk.ac.embl.ebi.ega.dataservice.endpoints.StatService;
import uk.ac.embl.ebi.ega.dataservice.endpoints.TicketService;
import uk.ac.embl.ebi.ega.dataservice.endpoints.UserDeleteService;
import uk.ac.embl.ebi.ega.dataservice.endpoints.UserPostService;
import uk.ac.embl.ebi.ega.dataservice.endpoints.UserService;
import uk.ac.embl.ebi.ega.dataservice.utils.Dailylog;
import uk.ac.embl.ebi.ega.dataservice.utils.DatabaseExecutor;
import uk.ac.embl.ebi.ega.dataservice.utils.RestyTimeOutOption;
import us.monoid.json.JSONArray;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;
import us.monoid.web.JSONResource;
import us.monoid.web.Resty;
import static us.monoid.web.Resty.content;
import static us.monoid.web.Resty.data;
import static us.monoid.web.Resty.delete;
import static us.monoid.web.Resty.form;

/**
 *
 * @author asenf
 */
public class EgaSecureDataService {

    private static final boolean SSL = false;
    private static int port = 9221;
    public static boolean testMode = false;

    private static Dailylog dailylog;
    
    // Database Access Object
    private static String dbe_path = "";
    private final DatabaseExecutor dbe;
    
    // Shutdown process: Wait until current operations complete
    static volatile boolean keepRunning = true;

    // Testing only
    private static String testuser;

    // Executors
    private final DefaultEventExecutorGroup l, s, r;
    private final ScheduledExecutorService executor;
    private final GlobalTrafficShapingHandler globalTrafficShapingHandler;
    
    public EgaSecureDataService(int port, int cores) {
        EgaSecureDataService.port = port;

        this.dbe = new DatabaseExecutor(dbe_path + "data.ini");

        // Executors
        this.l = new DefaultEventExecutorGroup(cores * 4);
        this.s = new DefaultEventExecutorGroup(cores);
        this.r = new DefaultEventExecutorGroup(cores * 4);
        
        // Traffic Shaping Handler already created
        this.executor = Executors.newScheduledThreadPool(cores);
        this.globalTrafficShapingHandler = new GlobalTrafficShapingHandler(executor, cores);
        this.globalTrafficShapingHandler.trafficCounter().configure(10000); // ??
    }
    
    public void run(HashMap<String, Service> mappings) throws Exception {
        // Configure SSL.
        final SslContext sslCtx;
        if (SSL) {
            SelfSignedCertificate ssc = new SelfSignedCertificate(); // DEVELOPMENT
            sslCtx = SslContext.newServerContext(SslProvider.JDK, ssc.certificate(), ssc.privateKey());
        } else {
            sslCtx = null;
        }
        
        EventLoopGroup acceptGroup = new NioEventLoopGroup();
        EventLoopGroup connectGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(acceptGroup, connectGroup)
             .channel(NioServerSocketChannel.class)
             //.handler(new LoggingHandler(LogLevel.INFO))
             .childHandler(new EgaSecureDataServiceInitializer(sslCtx, mappings, this.dbe, this.l, this.s, this.r, this.globalTrafficShapingHandler, this));

            Channel ch = b.bind(port).sync().channel();

            System.err.println("Open your web browser and navigate to " +
                    (SSL? "https" : "http") + "://127.0.0.1:" + port + '/');

            if (testMode)
                testMe();
        
            ch.closeFuture().sync();
        } finally {
            acceptGroup.shutdownGracefully();
            connectGroup.shutdownGracefully();
        }
    }

    // GET - Traffic Information
    public String getTransferString() {
        return this.globalTrafficShapingHandler.trafficCounter().toString();
    }
    public JSONObject getTransfer() {
        JSONObject traffic = new JSONObject();
        
        try {
            traffic.put("checkInterval", this.globalTrafficShapingHandler.trafficCounter().checkInterval());

            // Add more...
            
        } catch (JSONException ex) {;}
        
        return traffic;
    }
    
    /**
     * @param args the command line arguments
     * 
     * Parameters: port number (default 9221)
     *      -p port : server port (default 9221)
     *      -l path : path to ini file. default: . (local dir)
     *      -t user : testMe - run self-test (using specified user), then exit
     */
    public static void main(String[] args) {
        String p = "9221"; int pi = 9221;
        String testup = "";

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                keepRunning = false;
//                try {
//                    mainThread.join();
//                } catch (InterruptedException ex) {;}

                System.out.println("Shutdown!!");
            }
        });

        int cores = Runtime.getRuntime().availableProcessors();

        Options options = new Options();

        options.addOption("p", true, "port");        
        options.addOption("l", true, "path");        
        options.addOption("t", true, "testMe");        
        
        CommandLineParser parser = new BasicParser();
        try {        
            CommandLine cmd = parser.parse( options, args);
            
            if (cmd.hasOption("p"))
                p = cmd.getOptionValue("p");
            if (cmd.hasOption("l"))
                EgaSecureDataService.dbe_path = cmd.getOptionValue("l");
            if (cmd.hasOption("t")) {
                EgaSecureDataService.testMode = true;
                testup = cmd.getOptionValue("t");
                EgaSecureDataService.testuser = testup;
                System.out.println("User: " + EgaSecureDataService.testuser);
            }
            
            pi = Integer.parseInt(p);
        } catch (ParseException ex) {
            System.out.println("Unrecognized Parameter. Use '-p'  '-l'  -t'.");
            Logger.getLogger(EgaSecureDataService.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        // Add Service Endpoints
        DatasetService datasetService = new DatasetService();
        FileService fileService = new FileService();
        UserService userService = new UserService();
        UserPostService userPostService = new UserPostService();
        UserDeleteService userDeleteService = new UserDeleteService();
        TicketService ticketService = new TicketService();
        StatService statService = new StatService();
        
        HashMap<String, Service> mappings = new HashMap<>();
        mappings.put("/datasets", datasetService);
        mappings.put("/files", fileService);
        mappings.put("/users", userService);
        mappings.put("/userposts", userPostService); // Separate out database Inserts
        mappings.put("/userdeletes", userDeleteService); // Separate out database Deletes
        mappings.put("/tickets", ticketService);
        mappings.put("/stats", statService);
        
        // Set up Log File
        if (!EgaSecureDataService.testMode)
            EgaSecureDataService.dailylog = new Dailylog("data");

        // Start and run the server
        try {
            new EgaSecureDataService(pi, cores).run(mappings);
        } catch (Exception ex) {
            Logger.getLogger(EgaSecureDataService.class.getName()).log(Level.SEVERE, null, ex);
        }        
    }
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    public static void log(String text) { // Log to text file, daily
        if (EgaSecureDataService.dailylog != null) {
            String text_ = text;
            if (text_.toLowerCase().contains("pass")) {
                text_ = text_.substring(0, text.toLowerCase().indexOf("pass"));
            }
            EgaSecureDataService.dailylog.log(text_);
        }
    }
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    public static double getSystemCpuLoad() throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException {

        MBeanServer mbs    = ManagementFactory.getPlatformMBeanServer();
        ObjectName name    = ObjectName.getInstance("java.lang:type=OperatingSystem");
        AttributeList list = mbs.getAttributes(name, new String[]{ "SystemCpuLoad" });

        if (list.isEmpty())     return Double.NaN;

        Attribute att = (Attribute)list.get(0);
        Double value  = (Double)att.getValue();

        if (value == -1.0)      return Double.NaN;  // usually takes a couple of seconds before we get real values

        return ((int)(value * 1000) / 10.0);        // returns a percentage value with 1 decimal point precision
    }
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // Self-Test of functionality provided in this server
    private void testMe() throws Exception {
        // Wait until server has started up
        Thread.sleep(2000);
        
        Resty r = new Resty(new RestyTimeOutOption(8000,8000));

        String test_error = "Start";
        try {
            String username = EgaSecureDataService.testuser;
            String user = URLEncoder.encode(EgaSecureDataService.testuser, "UTF-8");
            String testuser = username;
            
            if (EgaSecureDataService.testuser != null && EgaSecureDataService.testuser.length() > 0) {
                // Test 1: List all Datasets
                test_error = "Test 1";
                String query1 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/datasets";
                JSONResource json1 = r.json(query1);
                JSONObject jobj1 = (JSONObject) json1.get("response");
                JSONArray jsonarr1 = (JSONArray)jobj1.get("result");
                System.out.println("Datasets: " + jsonarr1.length());

                ArrayList<String> datasets = new ArrayList<>();
                for (int i=0; i<jsonarr1.length(); i++)
                    datasets.add(jsonarr1.getString(i));

                // Test 2: List all files in all datasets
                // Pick first non-empty dataset for future test usage
                // Pick first file for future test usage
                test_error = "Test 2";
                String testDatasetID = "", testFileID = "";
                HashSet<String> emptyDatasets = new HashSet<>();
                for (String dataset : datasets) {
                    String query2 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/datasets/" + dataset + "/files";
                    JSONResource json2 = r.json(query2);
                    JSONObject jobj2 = (JSONObject) json2.get("response");
                    JSONArray jsonarr2 = (JSONArray)jobj2.get("result");
                    System.out.println("Files in Datasets " + dataset + ": " + jsonarr2.length());
                    // Print first 5 files for visual test reference
                    for (int j=0; j<(jsonarr2.length()>5?5:jsonarr2.length()); j++) {
                        JSONObject jsonObject2 = jsonarr2.getJSONObject(j);
                        
                        System.out.println("   " + j + ": " + jsonObject2.getString("fileID") + " :: " + jsonObject2.getString("fileName") + " :: " + jsonObject2.getLong("fileSize") + " :: " + jsonObject2.getString("fileStatus"));

                        if (testDatasetID.length() == 0)
                            testDatasetID = dataset.trim();
                        String fID = jsonObject2.getString("fileID");
                        if (testFileID.length() == 0 && (fID!=null && fID.length()>0) )
                            testFileID = fID.trim();
                    }
                    // If there are no files in this dataset, keep track to be able to avoid these for later tests
                    if (jsonarr2.length() == 0)
                        emptyDatasets.add(dataset);
                }
                
                // Test 15: List all users for a dataset
                test_error = "Test 15";
                String query15 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/datasets/" + testDatasetID + "/users";
                JSONResource json15 = r.json(query15);
                JSONObject jobj15 = (JSONObject) json15.get("response");
                JSONArray jsonarr15 = (JSONArray)jobj15.get("result");
                System.out.println("\nUsers for Datasets " + testDatasetID + ": " + jsonarr15.length());
                for (int j=0; j<jsonarr15.length(); j++) {
                    System.out.println("   " + j + ": " + jsonarr15.getString(j));
                }
                
                // Test 16: List access permission for users for a datasets
                test_error = "Test 16";
                String query16 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/datasets/" + testDatasetID + "/users/" + EgaSecureDataService.testuser;
                JSONResource json16 = r.json(query16);
                JSONObject jobj16 = (JSONObject) json16.get("response");
                JSONArray jsonarr16 = (JSONArray)jobj16.get("result");
                System.out.println("\nUser Access Permission for dataset " + testDatasetID);
                for (int j=0; j<jsonarr16.length(); j++) {
                    System.out.println("   " + j + ": " + jsonarr16.getString(j));
                }
                
                // Test 17: List permitted datasets for a user for a dac
                test_error = "Test 17";
                String query17 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/users/" + EgaSecureDataService.testuser + "/datasets/dac/" + "EGAC00001000105";
                JSONResource json17 = r.json(query17);
                JSONObject jobj17 = (JSONObject) json17.get("response");
                JSONArray jsonarr17 = (JSONArray)jobj17.get("result");
                System.out.println("\nDatasets for User (by DAC) " + testDatasetID);
                for (int j=0; j<jsonarr17.length(); j++) {
                    System.out.println("   " + j + ": " + jsonarr17.getString(j));
                }
                
                // Test 3: List details for 1 file
                test_error = "Test 3";
                if (testFileID.length() > 0) {
                    String query3 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/files/" + testFileID;
                    JSONResource json3 = r.json(query3);
                    JSONObject jobj3 = (JSONObject) json3.get("response");
                    JSONArray jsonarr3 = (JSONArray)jobj3.get("result");
                    JSONObject jobj31 = (JSONObject)jsonarr3.getJSONObject(0);

                    System.out.println("Result for " + testFileID);
                    System.out.println("   " + jobj31.getString("fileID"));
                    System.out.println("   " + jobj31.getString("fileName"));
                    System.out.println("   " + jobj31.getString("fileIndex"));
                    System.out.println("   " + jobj31.getString("fileDataset"));
                    System.out.println("   " + jobj31.getString("fileSize"));
                }
                
                // Test 4: User-Dataset (reset testDataset)
                test_error = "Test 4";
                testDatasetID = "";
                String query4 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/users/" + testuser + "/datasets";
                JSONResource json4 = r.json(query4);
                JSONObject jobj4 = (JSONObject) json4.get("response");
                JSONArray jsonarr4 = (JSONArray)jobj4.get("result");
                System.out.println("Datasets for user " + testuser + ": " + jsonarr4.length());
                
                // find first non-empty dataset for the specified test user - no screen output
                for (int i=0; i<jsonarr4.length(); i++) {
                    String datasetID = jsonarr4.getString(i);
                    if (!emptyDatasets.contains(datasetID)) {
                        testDatasetID = datasetID.trim();
                        break;
                    }
                }

                // Test 5: User-Dataset-Files
                test_error = "Test 5";
                if (testDatasetID.length() > 0) {
                    String query5 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/users/" + testuser + "/datasets/" + testDatasetID + "/files";
                    JSONResource json5 = r.json(query5);
                    JSONObject jobj5 = (JSONObject) json5.get("response");
                    JSONArray jsonarr5 = (JSONArray)jobj5.get("result");
                    System.out.println("Files in Datasets " + testDatasetID + " for user " + testuser + ": " + jsonarr5.length());
                }
                
                // Test 6: User-Requests-New (making a request)
                test_error = "Test 6";
                boolean request_ = false;
                if (testDatasetID.length() > 0) {
                    JSONObject json61 = new JSONObject();
                    json61.put("id", testDatasetID);
                    json61.put("rekey", "abc");
                    json61.put("downloadType", "STREAM");
                    json61.put("descriptor", "SelfTestRequest");

                    String query6 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/users/" + testuser + "/requests/download/dataset/" + testDatasetID;
                    JSONResource json6 = r.json(query6, form( data("downloadrequest", content(json61))) );
                    JSONObject jobj6 = (JSONObject) json6.get("response");
                    JSONArray jsonarr6 = (JSONArray)jobj6.get("result");
                    System.out.println("Requested Dataset " + testDatasetID + " for user " + testuser + ": " + jsonarr6.length() + " ticket.");
                    if (jsonarr6.length() > 0)
                        request_ = true;
                    for (int i=0; i<jsonarr6.length(); i++)
                        System.out.println("   :::   " + jsonarr6.getString(i));
                }

                // Test 7: User-Requests-New (single File) (making a request)
                test_error = "Test 7";
                if (testDatasetID.length() > 0) {
                    JSONObject json71 = new JSONObject();
                    json71.put("id", testFileID);
                    json71.put("rekey", "abc");
                    json71.put("downloadType", "STREAM");
                    json71.put("descriptor", "SelfTestRequestFile");

                    String query7 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/users/" + testuser + "/requests/download/file/" + testFileID;
                    JSONResource json7 = r.json(query7, form( data("downloadrequest", content(json71))) );
                    JSONObject jobj7 = (JSONObject) json7.get("response");
                    JSONArray jsonarr7 = (JSONArray)jobj7.get("result");
                    System.out.println("Requested File " + testFileID + " for user " + testuser + ": " + jsonarr7.length() + " ticket.");
                    for (int i=0; i<jsonarr7.length(); i++)
                        System.out.println("   :::   " + jsonarr7.getString(i));
                }
                
                // Test 8: User-Requests-Light (should now include 'SelfTestRequest')
                test_error = "Test 7.5";
                String query75 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/users/" + testuser + "/requestslight/";
                System.out.println("***" + query75);
                JSONResource json75 = r.json(query75);
                JSONObject jobj75 = (JSONObject) json75.get("response");
                JSONArray jsonarr75 = (JSONArray)jobj75.get("result");
                System.out.println("***RequestLight for user " + testuser + ": " + jsonarr75.length());
                for (int i=0; i<jsonarr75.length(); i++)
                    System.out.println("***\t" + jsonarr75.getString(i));
                
                // Test 8: User-Requests (should now include 'SelfTestRequest')
                test_error = "Test 8";
                String query8 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/users/" + testuser + "/requests/";
                JSONResource json8 = r.json(query8);
                JSONObject jobj8 = (JSONObject) json8.get("response");
                JSONArray jsonarr8 = (JSONArray)jobj8.get("result");
                System.out.println("Request Tickets for user " + testuser + ": " + jsonarr8.length());

                HashMap<String, Long> requests = new HashMap<>();
                for (int i=0; i<jsonarr8.length(); i++) {
                    JSONObject jsonObject81 = jsonarr8.getJSONObject(i);
                    String request8 = jsonObject81.getString("label");

                    long l = 1;
                    if (requests.containsKey(request8))
                        l = l + requests.get(request8);

                    requests.put(request8, l);
                }

                Iterator<String> iter = requests.keySet().iterator();
                while (iter.hasNext()) {
                    String key = iter.next();
                    System.out.println("\t" + key + ": " + requests.get(key));
                }

                if (request_) {
                    // Test 9: User-Requests-label-tickets, using test Request
                    test_error = "Test 9";
                    String testTicketID = "";
                    String query9 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/users/" + testuser + "/requests/request/" + "SelfTestRequest" + "/tickets";
                    JSONResource json9 = r.json(query9);
                    JSONObject jobj9 = (JSONObject) json9.get("response");
                    JSONArray jsonarr9 = (JSONArray)jobj9.get("result");
                    System.out.println("Request Tickets for user " + testuser + " and request " + "SelfTestRequest" + ": " + jsonarr9.length());
                    for (int i=0; i<jsonarr9.length(); i++) {
                        JSONObject ticket = (JSONObject) jsonarr9.get(i);
                        if (ticket != null && ticket.getString("ticket").length() > 0)
                            testTicketID = ticket.getString("ticket");
                    }                    

                    // Test 10: User-Requests-label-datasets
                    test_error = "Test 10";
                    String query10 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/users/" + testuser + "/requests/request/" + "SelfTestRequest" + "/datasets";
                    JSONResource json10 = r.json(query10);
                    JSONObject jobj10 = (JSONObject) json10.get("response");
                    JSONArray jsonarr10 = (JSONArray)jobj10.get("result");
                    System.out.println("Request Datasets for user " + testuser + " and request " + "SelfTestRequest" + ": " + jsonarr10.length());

                    for (int i=0; i<jsonarr10.length(); i++)
                        System.out.println("\t" + jsonarr10.getString(i));

                    // Test 11: User-Request single Ticket List
                    test_error = "Test 11";
                    if (testTicketID.length() > 0) {
                        String query11 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/users/" + testuser + "/requests/ticket/" + testTicketID;
                        JSONResource json11 = r.json(query11);
                        JSONObject jobj11 = (JSONObject) json11.get("response");
                        JSONArray jsonarr11 = (JSONArray)jobj11.get("result");
                        System.out.println("Request a Ticket for user " + testuser + " and ticket " + testTicketID + ": " + jsonarr11.length());
                        if (jsonarr11.length()>0)
                            System.out.println("   " + jsonarr11.get(0).toString());
                        
                        // Test 11.5 - list 1 ticket
                        String query115 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/tickets/" + testTicketID;
                        JSONResource json115 = r.json(query115);
                        JSONObject jobj115 = (JSONObject) json115.get("response");
                        JSONArray jsonarr115 = (JSONArray)jobj115.get("result");
                        System.out.println("Request a Ticket " + testTicketID + ": " + jsonarr115.length());
                        if (jsonarr115.length()>0)
                            System.out.println("   " + jsonarr115.get(0).toString());
                    }
                    
                    // Test 12 - localize (TODO)
                    
                    
                    
                    // Test 13: User-Ticket-Delete
                    test_error = "Test 13";
                    if (testTicketID.length() > 0) {
                        String query13 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/users/" + testuser + "/requests/delete/ticket/" + testTicketID;
                        JSONResource json13 = r.json(query13, delete());
                        JSONObject jobj13 = (JSONObject) json13.get("response");
                        JSONArray jsonarr13 = (JSONArray)jobj13.get("result");
                        System.out.println("Deleted tickets for user " + testuser + " and ticket " + testTicketID + ": " + jsonarr13.length());
                    }
                }
                
                // Test 14: User-Requests-Delete (also clean-up after self-test)
                test_error = "Test 14";
                String query14 = "http://localhost:" + EgaSecureDataService.port + "/ega/rest/data/v2/users/" + testuser + "/requests/delete/request/" + "SelfTestRequest";
                JSONResource json14 = r.json(query14, delete());
                JSONObject jobj14 = (JSONObject) json14.get("response");
                JSONArray jsonarr14 = (JSONArray)jobj14.get("result");
                System.out.println("Deleted tickets for user " + testuser + " and request " + "SelfTestRequest" + ": " + jsonarr14.length());

                for (int i=0; i<jsonarr14.length(); i++)
                    System.out.println(jsonarr14.get(i).toString());
                
            } else {
                System.out.println("User: " + EgaSecureDataService.testuser);
                System.out.println("Usage: '-t {username} for valid user.");
            }



            
        } catch (Throwable t) {
            System.out.println("Error at " + test_error + ": " + t.getLocalizedMessage());
        }
        
        System.exit(100); // End the server after self test is complete
    }
}
