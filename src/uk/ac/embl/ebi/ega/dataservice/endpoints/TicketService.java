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
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import uk.ac.embl.ebi.ega.dataservice.EgaSecureDataService;
import uk.ac.embl.ebi.ega.dataservice.utils.DatabaseExecutor;
import uk.ac.embl.ebi.ega.dataservice.utils.EgaTicket;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;

public class TicketService extends ServiceTemplate implements Service {

    @Override
    public JSONObject handle(ArrayList<String> id, Map<String, String> parameters, FullHttpRequest request, DatabaseExecutor dbe, EgaSecureDataService ref) {
        JSONObject json = new JSONObject(); // Start out with common JSON Object

        try {
            // /ticket/{ticket} [GET]
            String ticket = id.get(0);
            
            String ip = "";
            if (parameters.containsKey("ip"))
                ip = parameters.get("ip");
            
            EgaTicket[] oneTicket = dbe.getOneTicket(ticket);

            List<Map<String,String>> maps = new ArrayList<>();
            for (EgaTicket oneTicket1 : oneTicket) {
                maps.add(oneTicket1.getMap());
            }

            json.put("header", responseHeader(OK)); 
            json.put("response", responseSection(maps));
        } catch (Exception ex) {
            Logger.getLogger(TicketService.class.getName()).log(Level.SEVERE, null, ex);
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
