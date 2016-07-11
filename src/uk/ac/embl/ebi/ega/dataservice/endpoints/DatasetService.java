/*
 * Copyright 2014 EMBL-EBI.
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
import java.util.logging.Level;
import java.util.logging.Logger;
import uk.ac.embl.ebi.ega.dataservice.EgaSecureDataService;
import uk.ac.embl.ebi.ega.dataservice.utils.DatabaseExecutor;
import uk.ac.embl.ebi.ega.dataservice.utils.EgaFile;
import uk.ac.embl.ebi.ega.dataservice.utils.EgaOnePermission;
import uk.ac.embl.ebi.ega.dataservice.utils.EgaPermission;
import us.monoid.json.JSONArray;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;

/**
 *
 * @author asenf
 */
public class DatasetService extends ServiceTemplate implements Service {

    @Override
    public JSONObject handle(ArrayList<String> id, Map<String, String> parameters, FullHttpRequest request, DatabaseExecutor dbe, EgaSecureDataService ref) {
        JSONObject json = new JSONObject(); // Start out with common JSON Object

        try {
            // /datasets                            all datasets
            // /datasets/{datasetid}/files          all files in {datasetid}
            // /datasets/{datasetid}/users          all users authorized for {datasetid}
            // /datasets/{datasetid}/users/{email}  is {email} authorized for {datasetid}?

            if (id==null || id.size()==0) { // dataset ID not specified: list all datasets
                String[] result = dbe.getDatasets();
                json.put("response", responseSection(result));
            } else {                        // list file in specified dataset
                String datasetid = id.get(0);                
                boolean allowed = false;
                if (id.size()>=2 && id.get(1).equalsIgnoreCase("files")) { 
                    EgaFile[] result = dbe.getFilesByDataset(datasetid);                    
                    json.put("response", responseSection(result));
                } else if (id.size()>=2 && id.get(1).equalsIgnoreCase("users")) { 
                    EgaPermission[] result = dbe.getUsersByDataset(datasetid);
                    if (id.size()>=3 && id.get(2).length() > 0) { // User specified
                        String email = id.get(2);
                        String permission_status = "null";
                        String permission_date = "null";
                        String changed_by = "null";
                        for (int i=0; i<result.length; i++) {
                            if (result[i].getUser().equalsIgnoreCase(email)) {
                                permission_status = result[i].getPermissionStatus();
                                allowed = permission_status.equalsIgnoreCase("approved");
                                permission_date = result[i].getPermissionDate();
                                changed_by = result[i].getChangedBy();
                            }
                        }

                        EgaOnePermission result_ = new EgaOnePermission(allowed?"yes":"no", permission_date, changed_by);
                        json.put("response", responseSection(result_.getMap()));
                    } else { // List all users!
                        String[] result_ = new String[result.length];
                        for (int u=0; u<result.length; u++)
                            result_[u] = result[u].getUser();
                        json.put("response", responseSection(result_));
                    }
                }
            }
            
            json.put("header", responseHeader(OK)); // Header Section of the response
            //json.put("response", responseSection(result));            
        } catch (JSONException ex) {
            Logger.getLogger(DatasetService.class.getName()).log(Level.SEVERE, null, ex);
            try {
                json.put("header", responseHeader(SEE_OTHER, ex.getLocalizedMessage()));
                //json.put("response", responseSection(null));            
            } catch (JSONException ex1) {
                Logger.getLogger(StatService.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }

        return json;
    }
    
    private JSONObject responseSection(EgaFile[] files) throws JSONException {
        JSONObject response = new JSONObject();

        response.put("numTotalResults", files.length);
        response.put("resultType", "us.monoid.json.JSONArray");
        
        JSONArray arr = new JSONArray();
        for (int i=0; i<files.length; i++)
            arr.put(files[i].getMap());
        
        JSONArray mJSONArray = files!=null?arr:new JSONArray();        
        //JSONArray mJSONArray = files!=null?new JSONArray(Arrays.asList(files)):new JSONArray();        
        response.put("result", mJSONArray);
        
        return response;        
    }
}
