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

package uk.ac.embl.ebi.ega.dataservice.endpoints;

import io.netty.handler.codec.http.FullHttpRequest;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SEE_OTHER;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import uk.ac.embl.ebi.ega.dataservice.EgaSecureDataService;
import uk.ac.embl.ebi.ega.dataservice.utils.DatabaseExecutor;
import uk.ac.embl.ebi.ega.dataservice.utils.EgaFile;
import uk.ac.embl.ebi.ega.dataservice.utils.EgaTicket;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;

public class UserService extends ServiceTemplate implements Service {

    @Override
    public JSONObject handle(ArrayList<String> id, Map<String, String> parameters, FullHttpRequest request, DatabaseExecutor dbe, EgaSecureDataService ref) {
        JSONObject json = new JSONObject(); // Start out with common JSON Object

        try {
            // /users/{email}/requests [GET]
            // /users/{email}/requestslight [GET]
            // /users/{email}/requests/request/{label}/datasets [GET]
            // /users/{email}/requests/request/{label}/tickets [GET]
            // /users/{email}/requests/request/{label}/localize [GET]
            // /users/{email}/requests/ticket/{ticket} [GET]
            
            // /users/{email}/datasets [GET]
            // /users/{email}/datasets/{datasetid}/files [GET]
            // /users/{email}/datasets/dac/{dac}
            
            // Get user information (email)
            String email = null;
            if (id!=null && id.size()>=1)
                email = id.get(0);          // User Email
            else
                throw new Exception("Email not available");

            // Get intended operation (function)
            String function = null;
            if (id.size()>=2)
                function = id.get(1);       // Function: 'requests' or 'datasets'
            else
                throw new Exception("Function not available");

            String ip = "";
            if (parameters.containsKey("ip"))
                ip = parameters.get("ip");
            
            // Perform specific user function for datasets or requests
            if (function.equalsIgnoreCase("requests"))
                handleRequest(email, id, ip, dbe, json);
            else if (function.equalsIgnoreCase("requestslight"))
                handleRequestLight(email, id, ip, dbe, json);
            else if (function.equalsIgnoreCase("datasets"))
                handleDataset(email, id, dbe, json);
            else
                throw new Exception("Unknown Function");
            
        } catch (Exception ex) {
            Logger.getLogger(UserService.class.getName()).log(Level.SEVERE, null, ex);
            try {
                json.put("header", responseHeader(SEE_OTHER, ex.getLocalizedMessage()));
                //json.put("response", responseSection(null));            
            } catch (JSONException ex1) {
                Logger.getLogger(StatService.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }

        return json;
    }

    // List all Datasets for a User
    // List files in a dataset for a User [convenience]
    private void handleDataset(String email, ArrayList<String> id, DatabaseExecutor dbe, JSONObject json) throws JSONException {
        String[] datasets = null;
        
        if (id.size() >= 4 && id.get(2).equalsIgnoreCase("dac")) {
            String DAC = id.get(3);
            datasets = dbe.getDatasetsByUser(email, DAC);
        } else
            datasets = dbe.getDatasetsByEmail(email);
   
        if (id.size() == 2 || (id.size() >= 4 && id.get(2).equalsIgnoreCase("dac"))) { // All User Datasets
            json.put("header", responseHeader(OK)); 
            json.put("response", responseSection(datasets));
            
        } else if (id.size() == 4) { // Files in a Dataset - verify User access to Dataset
            ArrayList<String> datasetList = new ArrayList(Arrays.asList(datasets));
            String dataset = id.get(2);
            if (id.get(3).equalsIgnoreCase("files") && datasetList.contains(dataset)) {
                EgaFile[] filesByDataset = dbe.getFilesByDataset(dataset);
                
                List<Map<String,String>> maps = new ArrayList<>();
                for (EgaFile filesByDataset1 : filesByDataset) {
                    maps.add(filesByDataset1.getMap());
                }
                
                json.put("header", responseHeader(OK)); 
                json.put("response", responseSection(maps));
            }
        }
    }

    // List all Requests for a User
    // List all Request Tickets for a Request for a User (similar to File)
    // List Ticket Details for a Ticket for a User (similar to File)
    private void handleRequest(String email, ArrayList<String> id, String ip, DatabaseExecutor dbe, JSONObject json) throws JSONException {
        
        if (id.size() == 2) { // list all requests (tickets)
            EgaTicket[] ticketsByEmail = dbe.getTicketsByEmail(email);
        
            List<Map<String,String>> maps = new ArrayList<>();
            for (EgaTicket ticketsByEmail1 : ticketsByEmail) {
                maps.add(ticketsByEmail1.getMap());
            }

            // Just list Requests IDs?
            
            json.put("header", responseHeader(OK)); 
            json.put("response", responseSection(maps));
            
        } else if (id.size() > 2) {
            String type = id.get(2);
            
            if (type.equalsIgnoreCase("request")) { // ------------------------- Request
                String label = id.get(3);
                String info = id.get(4);
                
                if (info.equalsIgnoreCase("datasets")) { // -------- Datasets in the Request
                    String[] datasets = dbe.getDatasetsByRequest(label);
                    json.put("header", responseHeader(OK));
                    json.put("response", responseSection(datasets));
                    
                } else if (info.equalsIgnoreCase("tickets")) { // -- Tickets in the Request
                    EgaTicket[] ticketsByRequest = dbe.getTicketsByRequest(email, label);

                    List<Map<String,String>> maps = new ArrayList<>();
                    for (EgaTicket ticketsByRequest1 : ticketsByRequest) {
                        maps.add(ticketsByRequest1.getMap());
                    }

                    json.put("header", responseHeader(OK)); 
                    json.put("response", responseSection(maps));
                    
                } else if (info.equalsIgnoreCase("localize")) { // -- Localize the Request to current IP
                    dbe.localizeRequest(email, label, ip);
                    
                    String[] resp = new String[]{"Request Localized"};

                    json.put("header", responseHeader(OK));
                    json.put("response", responseSection(resp));
                    
                }
                
            } else if (type.equalsIgnoreCase("ticket")) { // ------------------- Ticket (one)
                String ticket = id.get(3);
                
                EgaTicket[] oneTicket = dbe.getOneTicket(email, ticket);

                List<Map<String,String>> maps = new ArrayList<>();
                for (EgaTicket oneTicket1 : oneTicket) {
                    maps.add(oneTicket1.getMap());
                }

                json.put("header", responseHeader(OK)); 
                json.put("response", responseSection(maps));
                
            }
            
        }
        
    }

    private void handleRequestLight(String email, ArrayList<String> id, String ip, DatabaseExecutor dbe, JSONObject json) throws JSONException {
        
        if (id.size() == 2) { // list all requests (tickets)
            String[] t = dbe.getRequestsLight(email);
        
            json.put("header", responseHeader(OK)); 
            json.put("response", responseSection(t));            
        }
    }

}
