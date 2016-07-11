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
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import uk.ac.embl.ebi.ega.dataservice.EgaSecureDataService;
import uk.ac.embl.ebi.ega.dataservice.utils.DatabaseExecutor;
import uk.ac.embl.ebi.ega.dataservice.utils.EgaFile;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;

public class UserPostService extends ServiceTemplate implements Service {

    @Override
    public JSONObject handle(ArrayList<String> id, Map<String, String> parameters, FullHttpRequest request, DatabaseExecutor dbe, EgaSecureDataService ref) {
        JSONObject json = new JSONObject(); // Start out with common JSON Object

        try {
            // /userposts/{email}/requests/download/file/{fileid} [POST]
            // /userposts/{email}/requests/download/dataset/{datasetid} [POST]
            
            // Get user information (email)
            String email = null;
            if (id!=null && id.size()>=1)
                email = id.get(0);          // User Email
            else
                throw new Exception("Email not available");

            // Get intended operation (function)
            String function = null;
            if (id.size()>=2)
                function = id.get(1);       // Function: 'requests'
            else
                throw new Exception("Function not available");

            // Perform specific user function for datasets or requests
            if (function.equalsIgnoreCase("requests"))
                handleRequest(email, id, dbe, json, parameters);
            else
                throw new Exception("Unknown Function");
            
        } catch (Exception ex) {
            Logger.getLogger(UserPostService.class.getName()).log(Level.SEVERE, null, ex);
            try {
                json.put("header", responseHeader(SEE_OTHER, ex.getLocalizedMessage()));
                //json.put("response", responseSection(null));            
            } catch (JSONException ex1) {
                Logger.getLogger(StatService.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }

        return json;
    }

    // List all Requests for a User
    // List all Request Tickets for a Request for a User (similar to File)
    // List Ticket Details for a Ticket for a User (similar to File)
    private void handleRequest(String email, ArrayList<String> id, DatabaseExecutor dbe, JSONObject json, Map<String, String> parameters) throws JSONException {
        
        // /download/file/{fileid} [POST]
        // /download/dataset/{datasetid} [POST]

        if (id.size() >= 5 && id.get(2).equalsIgnoreCase("download")) { // POST new download Request
            
            // Get All Files in this request
            String type = id.get(3); // 'file' or 'dataset'
            String id_ = id.get(4); // fileid or datasetid
            
            // Get all files reference by the request
            EgaFile[] files = null;
            if (type.equalsIgnoreCase("file"))
                files = dbe.getFile(id_);
            else if (type.equalsIgnoreCase("dataset"))
                files = dbe.getFilesByDataset(id_);

            int insertRequest = -1;
            if (files != null && files.length > 0) { // If there are any files
                // Prepare Arrays for Batch Update
                ArrayList<String> account_stable_id = new ArrayList<>(), 
                                  download_ticket = new ArrayList<>(), 
                                  session_token = new ArrayList<>(), 
                                  client_ip = new ArrayList<>(), 
                                  file_stable_id = new ArrayList<>(), 
                                  file_type = new ArrayList<>(), 
                                  encryption_key = new ArrayList<>(), 
                                  status = new ArrayList<>(), 
                                  group_name = new ArrayList<>(), 
                                  type_ = new ArrayList<>(), 
                                  target = new ArrayList<>();

                String reKey = parameters.get("rekey");
                String downloadtype = parameters.get("downloadType");
                String descriptor = parameters.get("descriptor");
                String target_ = "AES128";
                String ip = parameters.get("ip");

                for (EgaFile oneFile: files) {
                    if (oneFile.getFileSize() > 0) { // Skip pending files -- doesn't work any longer!
                        if (oneFile.getStatus().length()>0) { // This conttains the MD5 - check that is is there...
                            if (type.equalsIgnoreCase("file") && file_stable_id.contains(oneFile.getFileID())) { // Skip files present in multiple datasets
                                continue;
                            }
                            
                            // Produce Ticket
                            UUID idOne = UUID.randomUUID();
                            String ticket = idOne.toString();

                            account_stable_id.add(email);
                            download_ticket.add(ticket);
                            session_token.add("none/shiro");
                            client_ip.add(ip);
                            file_stable_id.add(oneFile.getFileID());
                            file_type.add("EGA-LOCAL");
                            encryption_key.add(reKey);
                            status.add("ready");
                            group_name.add(descriptor);
                            type_.add(downloadtype);
                            target.add(target_);
                        }
                    }
                }

                // Call dbe function to store request by id
                insertRequest = dbe.insertRequestBatch(account_stable_id, 
                                                       download_ticket, 
                                                       session_token, 
                                                       client_ip, 
                                                       file_stable_id, 
                                                       file_type, 
                                                       encryption_key, 
                                                       status, 
                                                       group_name, 
                                                       type_, 
                                                       target);
            }
            
            json.put("header", responseHeader(OK)); 
            json.put("response", responseSection(new String[]{String.valueOf(insertRequest)}));
        }        
    }
}
