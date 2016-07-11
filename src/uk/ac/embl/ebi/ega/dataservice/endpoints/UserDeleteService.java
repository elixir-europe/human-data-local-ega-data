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
import java.util.logging.Level;
import java.util.logging.Logger;
import uk.ac.embl.ebi.ega.dataservice.EgaSecureDataService;
import uk.ac.embl.ebi.ega.dataservice.utils.DatabaseExecutor;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;

public class UserDeleteService extends ServiceTemplate implements Service {

    @Override
    public JSONObject handle(ArrayList<String> id, Map<String, String> parameters, FullHttpRequest request, DatabaseExecutor dbe, EgaSecureDataService ref) {
        JSONObject json = new JSONObject(); // Start out with common JSON Object

        try {
            // /userdeletes/{email}/requests/delete/request/{label} [DELETE]
            // /userdeletes/{email}/requests/delete/ticket/{ticket} [DELETE]
            
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
            String ip = parameters.get("ip");
            if (function.equalsIgnoreCase("requests"))
                handleRequest(email, id, dbe, json, ip);
            else
                throw new Exception("Unknown Function");
            
        } catch (Exception ex) {
            Logger.getLogger(UserDeleteService.class.getName()).log(Level.SEVERE, null, ex);
            try {
                json.put("header", responseHeader(SEE_OTHER, ex.getLocalizedMessage()));
                //json.put("response", responseSection(null));            
            } catch (JSONException ex1) {
                Logger.getLogger(StatService.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }

        return json;
    }

    private void handleRequest(String email, ArrayList<String> id, DatabaseExecutor dbe, JSONObject json, String ip) throws JSONException {

        if (id.size() >= 2) { // list all requests (tickets)
            if (!id.get(2).equalsIgnoreCase("delete"))
                return;
            
            String type = id.get(3); // 'request' or 'ticket'
            String id_ = id.get(4); // ID to be deleted

            if (type.equalsIgnoreCase("request"))
                dbe.deleteRequest(id_, ip);
            else if (type.equalsIgnoreCase("ticket"))
                dbe.deleteTicket(id_, ip);
            
            json.put("header", responseHeader(OK)); 
            json.put("response", responseSection(new String[]{"OK"}));            
        }        
    }
}
