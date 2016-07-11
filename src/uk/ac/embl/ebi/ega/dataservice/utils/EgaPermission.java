/*
 * Copyright 2016 EMBL-EBI.
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
package uk.ac.embl.ebi.ega.dataservice.utils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author asenf
 */
public class EgaPermission {
    private String user = null;
    private String permission_status = null;
    private String permission_date = null;
    private String changed_by = null;
    
    public EgaPermission(String user, String permission_status, String permission_date, String changed_by) {
        this.user = user;
        this.permission_status = permission_status;
        this.permission_date = permission_date;
        this.changed_by = changed_by;
    }
    
    public void setuser(String user) {
        this.user = user;
    }
    
    public void setPermissionStatus(String permission_status) {
        this.permission_status = permission_status;
    }

    public void setPermissionDate(String permission_date) {
        this.permission_date = permission_date;
    }

    public void setChangedBy(String changed_by) {
        this.changed_by = changed_by;
    }
    
    public String getUser() {
        return this.user;
    }
    
    public String getPermissionStatus() {
        return this.permission_status;
    }

    public String getPermissionDate() {
        return this.permission_date;
    }

    public String getChangedBy() {
        return this.changed_by;
    }
    
    public Map<String,String> getMap() {
        Map<String,String> result = new LinkedHashMap<>();

        result.put("user", this.user);
        result.put("permission_status", this.permission_status);
        result.put("permission_date", this.permission_date);
        result.put("changed_by", this.changed_by);
                
        return result;
    }
    
}
