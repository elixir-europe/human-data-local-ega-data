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
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;

/**
 *
 * @author asenf
 */
public class FileService extends ServiceTemplate implements Service {

    @Override
    public JSONObject handle(ArrayList<String> id, Map<String, String> parameters, FullHttpRequest request, DatabaseExecutor dbe, EgaSecureDataService ref) {
        JSONObject json = new JSONObject(); // Start out with common JSON Object
        
        try {
            // /files/{fileid}              file details for {fileId}
            // /files/{fileid}/details      file enhanced details for {fileId}
            
            if (id!=null || id.size()==1) { // dataset ID not specified: list all datasets
                String fileid = id.get(0);
                EgaFile[] result = dbe.getFile(fileid);
                if (result!=null && result.length>0)
                    json.put("response", responseSection(result[0].getMap())); // 1 result for 1 file request
            } else if (id!=null || id.size()>1) {
                String fileid = id.get(0);
                if (id.get(1).equalsIgnoreCase("details")) { // ...or maybe list individual elements of the file details
                    // TODO
                    // Get local DB path
                }
            }

            json.put("header", responseHeader(OK)); // Header Section of the response
            //json.put("response", responseSection(result));            
        } catch (JSONException ex) {
            Logger.getLogger(FileService.class.getName()).log(Level.SEVERE, null, ex);
            try {
                json.put("header", responseHeader(SEE_OTHER, ex.getLocalizedMessage()));
                //json.put("response", responseSection(null));            
            } catch (JSONException ex1) {
                Logger.getLogger(StatService.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }

        return json;
    }
}
