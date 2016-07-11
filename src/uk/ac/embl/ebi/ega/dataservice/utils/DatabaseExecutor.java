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
package uk.ac.embl.ebi.ega.dataservice.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;

/**
 *
 * @author asenf
 */
public class DatabaseExecutor {
    // Ini file location default
    private String iniFile = "data.ini";
    
    // Database Pool
    private DataSource dbSource;
    
    // Query Strings (populated from ini file)
    private String datasets = null;
    private String datasets_by_email = null;
    private String files = null;
    private String files_by_dataset = null;
    private String requests_by_email = null;
    private String datasets_by_request = null;
    private String tickets_by_email = null;
    private String tickets_by_request = null;
    private String one_ticket = null;
    private String one_ticket_only = null;
    private String insert_ticket = null;
    private String delete_ticket = null;
    private String delete_request = null;
    private String localize_request = null;
    private String dataset_users = null;
    private String user_datasets_dac = null;
    private String user_dataset = null;
    private String requests_light = null;
    
    public DatabaseExecutor(String iniFile) {
        if (iniFile!=null)
            this.iniFile = iniFile;
        
        // Read Ini File, configure database
        Ini ini = null;
        try {
            ini = new Ini(new File(this.iniFile));
        } catch (IOException ex) {
            Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Read initialization file 
        if (ini != null) {
            // Database Connection ---------------------------------------------
            Section section = ini.get("database");
            
            // Configure database pool with it
            String instance = "";
            if (section.containsKey("instance"))
                instance = section.get("instance");
            String port = "";
            if (section.containsKey("port"))
                    port = section.get("port");
            String database = "";
            if (section.containsKey("database"))
                database = section.get("database");
            String user = "";
            if (section.containsKey("username"))
                user = section.get("username");
            String pass = "";
            if (section.containsKey("password"))
                pass = section.get("password");
            
            this.dbSource = MyDataSourceFactory.getHikariDataSource(instance, port, database, user, pass);
            
            // Populate query strings with it ----------------------------------
            Section queries = ini.get("queries");
            
            if (queries.containsKey("all_datasets"))
                this.datasets = queries.get("all_datasets");
            if (queries.containsKey("all_datasets_by_email"))
                this.datasets_by_email = queries.get("all_datasets_by_email");
            if (queries.containsKey("files"))
                this.files = queries.get("files");
            if (queries.containsKey("files_by_dataset"))
                this.files_by_dataset = queries.get("files_by_dataset");
            if (queries.containsKey("requests_by_email"))
                this.requests_by_email = queries.get("requests_by_email");
            if (queries.containsKey("all_datasets_by_request"))
                this.datasets_by_request = queries.get("all_datasets_by_request");
            if (queries.containsKey("all_tickets_by_email"))
                this.tickets_by_email = queries.get("all_tickets_by_email");
            if (queries.containsKey("all_tickets_by_request"))
                this.tickets_by_request = queries.get("all_tickets_by_request");
            if (queries.containsKey("one_ticket"))
                this.one_ticket = queries.get("one_ticket");
            if (queries.containsKey("one_ticket_only"))
                this.one_ticket_only = queries.get("one_ticket_only");
            if (queries.containsKey("insert_ticket"))
                this.insert_ticket = queries.get("insert_ticket");
            if (queries.containsKey("delete_ticket"))
                this.delete_ticket = queries.get("delete_ticket");
            if (queries.containsKey("delete_request"))
                this.delete_request = queries.get("delete_request");
            if (queries.containsKey("localize_request"))
                this.localize_request = queries.get("localize_request");            
            if (queries.containsKey("dataset_users"))
                this.dataset_users = queries.get("dataset_users");
            if (queries.containsKey("user_datasets_dac"))
                this.user_datasets_dac = queries.get("user_datasets_dac");
            if (queries.containsKey("user_dataset"))
                this.user_dataset = queries.get("user_dataset");
            if (queries.containsKey("requests_light"))
                this.requests_light = queries.get("requests_light");
        }
    }
    
    // -------------------------------------------------------------------------
    // --- Getters and Setters -------------------------------------------------
    // -------------------------------------------------------------------------

    // TODO
    
    // -------------------------------------------------------------------------
    // --- Database Execution Functions ----------------------------------------
    // -------------------------------------------------------------------------
    
    
    public String[] getDatasets() {
        String[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<String> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.datasets);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results
                while (rs.next()) {
                    String set = rs.getString(1);
                    resultset.add(set);
                }
                
                // Place result in Array
                result = resultset.toArray(new String[resultset.size()]);
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    public String[] getDatasetsByEmail(String email) {
        String[] result = null;

        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<String> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.datasets_by_email);
                ps.setString(1, email);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results
                while (rs.next()) {
                    String set = rs.getString(1);
                    resultset.add(set);
                }
                
                // Place result in Array
                result = resultset.toArray(new String[resultset.size()]);
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    public EgaFile[] getFile(String files) {
        EgaFile[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<EgaFile> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.files);
                ps.setString(1, files);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results: stable_id, file_name, index_name, size
                while (rs.next()) {
                    String stableid = rs.getString(1);
                    String datasetid = rs.getString(2);
                    String filename = rs.getString(3);
                    if (filename.toLowerCase().startsWith("/fire/A/ega/vol1")) filename = filename.substring(16);
                    String indexname = rs.getString(4);
                    String size = rs.getString(5);
                    String md5 = "TODO: MD5";
                    String status = rs.getString(6);
                    
                    long lSize = (size!=null&&size.length()>0)?Long.parseLong(size.trim()):-1L;
                    
                    if (!stableid.equalsIgnoreCase("TEMP"))
                        resultset.add(new EgaFile(stableid, filename, indexname, datasetid, lSize, md5, status));
                }
                
                
                // Place result in Array
                result = resultset.toArray(new EgaFile[resultset.size()]);
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    public EgaFile[] getFilesByDataset(String dataset) {
        EgaFile[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<EgaFile> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.files_by_dataset);
                ps.setString(1, dataset);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results: stable_id, file_name, index_name, size
                while (rs.next()) {
                    String stableid = rs.getString(1);
                    String filename = rs.getString(2);
                    if (filename.toLowerCase().startsWith("/fire/a/ega/vol1")) filename = filename.substring(16);
                    String indexname = rs.getString(3);
                    String size = rs.getString(4);
                    String md5 = "TODO: MD5";
                    String status = rs.getString(5);
                    
                    long lSize = (size!=null&&size.length()>0)?Long.parseLong(size.trim()):-1L;
                    
                    if (!stableid.equalsIgnoreCase("TEMP"))
                        resultset.add(new EgaFile(stableid, filename, indexname, dataset, lSize, md5, status));
                }
                                
                // Place result in Array
                result = resultset.toArray(new EgaFile[resultset.size()]);
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    public String[] getRequestsByEmail(String email) {
        String[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<String> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.requests_by_email);
                ps.setString(1, email);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results
                while (rs.next())
                    resultset.add(rs.getString(1));
                
                // Place result in Array
                result = resultset.toArray(new String[resultset.size()]);               
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    public String[] getDatasetsByRequest(String request) {
        String[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<String> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.datasets_by_request);
                ps.setString(1, request);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results                
                while (rs.next())
                    resultset.add(rs.getString(1));
                
                // Place result in Array
                result = resultset.toArray(new String[resultset.size()]);               
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    public EgaTicket[] getTicketsByEmail(String email) {
        EgaTicket[] result = null;

        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<EgaTicket> resultset = new ArrayList<>();

                ps = conn.prepareStatement(this.tickets_by_email);
                ps.setString(1, email);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results
                while (rs.next()) {
                    String ticket = rs.getString(1);
                    String request = rs.getString(2);
                    String fileID = rs.getString(3);
                    String fileType = rs.getString(4);
                    String fileSize = rs.getString(5);
                    String fileName = rs.getString(6);
                    if (fileName.toLowerCase().startsWith("/fire/a/ega/vol1")) fileName = fileName.substring(16);
                    String encryptionKey = rs.getString(7);
                    String transferType = rs.getString(8);
                    String transferTarget = rs.getString(9);
                    String user = rs.getString(10);
                    
                    resultset.add(new EgaTicket(ticket, request, fileID, fileType, fileSize, fileName, encryptionKey, transferType, transferTarget, user));
                }
                
                // Place result in Array
                result = resultset.toArray(new EgaTicket[resultset.size()]);
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        } else {
            System.out.println("NULL CONNECTION DB");
        }
        
        System.out.println("All Tickets: " + result.length);
        return result;
    }
    
    public EgaTicket[] getTicketsByRequest(String email, String request) {
        EgaTicket[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<EgaTicket> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.tickets_by_request);
                ps.setString(1, email);
                ps.setString(2, request);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results
                while (rs.next()) {
                    String ticket = rs.getString(1);
                    String fileID = rs.getString(2);
                    String fileType = rs.getString(3);
                    String fileSize = rs.getString(4);
                    String fileName = rs.getString(5);
                    if (fileName.toLowerCase().startsWith("/fire/a/ega/vol1")) fileName = fileName.substring(16);
                    String encryptionKey = rs.getString(6);
                    String transferType = rs.getString(7);
                    String transferTarget = rs.getString(8);
                    String user = rs.getString(9);
                    
                    resultset.add(new EgaTicket(ticket, request, fileID, fileType, fileSize, fileName, encryptionKey, transferType, transferTarget, user));
                }
                
                // Place result in Array
                result = resultset.toArray(new EgaTicket[resultset.size()]);
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    public EgaTicket[] getOneTicket(String email, String ticket) {
        EgaTicket[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<EgaTicket> resultset = new ArrayList<>();

                ps = conn.prepareStatement(this.one_ticket);
                ps.setString(1, email);
                ps.setString(2, ticket);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results
                while (rs.next()) {
                    String request = rs.getString(1);
                    String fileID = rs.getString(2);
                    String fileType = rs.getString(3);
                    String fileSize = rs.getString(4);
                    String fileName = rs.getString(5);
                    if (fileName.toLowerCase().startsWith("/fire/a/ega/vol1")) fileName = fileName.substring(16);
                    String encryptionKey = rs.getString(6);
                    String transferType = rs.getString(7);
                    String transferTarget = rs.getString(8);
                    String user = rs.getString(9);
                    
                    resultset.add(new EgaTicket(ticket, request, fileID, fileType, fileSize, fileName, encryptionKey, transferType, transferTarget, user));
                }
                
                // Place result in Array
                result = resultset.toArray(new EgaTicket[resultset.size()]);
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    public EgaTicket[] getOneTicket(String ticket) {
        EgaTicket[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<EgaTicket> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.one_ticket_only);
                ps.setString(1, ticket);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results
                while (rs.next()) {
                    String request = rs.getString(1);
                    String fileID = rs.getString(2);
                    String fileType = rs.getString(3);
                    String fileSize = rs.getString(4);
                    String fileName = rs.getString(5);
                    if (fileName.toLowerCase().startsWith("/fire/a/ega/vol1")) fileName = fileName.substring(16);
                    String encryptionKey = rs.getString(6);
                    String transferType = rs.getString(7);
                    String transferTarget = rs.getString(8);
                    String user = rs.getString(9);
                    
                    resultset.add(new EgaTicket(ticket, request, fileID, fileType, fileSize, fileName, encryptionKey, transferType, transferTarget, user));
                }
                
                // Place result in Array
                result = resultset.toArray(new EgaTicket[resultset.size()]);
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }

    public int insertRequest(String account_stable_id, String download_ticket, String session_token, String client_ip, String file_stable_id, String file_type, String encryption_key, String status, String group_name, String type, String target) { // batch!
        int result = -1;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            try {
                ps = conn.prepareStatement(this.insert_ticket);
                ps.setString(1, account_stable_id);
                ps.setString(2, download_ticket);
                ps.setString(3, session_token);
                ps.setString(4, client_ip);
                ps.setString(5, file_stable_id);
                ps.setString(6, file_type);
                ps.setString(7, encryption_key);
                ps.setString(8, status);
                ps.setString(9, group_name);
                ps.setString(10, target);
        
                // Execute query
                result = ps.executeUpdate();
                
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(ps);
            }
        }
        
        return result;
    }

    // Convenience - conversion from ArrayList to Array
    public int insertRequestBatch(ArrayList<String> account_stable_id, 
                                  ArrayList<String> download_ticket, 
                                  ArrayList<String> session_token, 
                                  ArrayList<String> client_ip, 
                                  ArrayList<String> file_stable_id, 
                                  ArrayList<String> file_type, 
                                  ArrayList<String> encryption_key, 
                                  ArrayList<String> status, 
                                  ArrayList<String> group_name, 
                                  ArrayList<String> type, 
                                  ArrayList<String> target) { // batch!
        return insertRequestBatch(account_stable_id.toArray(new String[account_stable_id.size()]),
                                  download_ticket.toArray(new String[download_ticket.size()]),
                                  session_token.toArray(new String[session_token.size()]),
                                  client_ip.toArray(new String[client_ip.size()]), 
                                  file_stable_id.toArray(new String[file_stable_id.size()]), 
                                  file_type.toArray(new String[file_type.size()]), 
                                  encryption_key.toArray(new String[encryption_key.size()]), 
                                  status.toArray(new String[status.size()]), 
                                  group_name.toArray(new String[group_name.size()]), 
                                  type.toArray(new String[type.size()]), 
                                  target.toArray(new String[target.size()]));
    }
    public int insertRequestBatch(String[] account_stable_id, 
                                  String[] download_ticket, 
                                  String[] session_token, 
                                  String[] client_ip, 
                                  String[] file_stable_id, 
                                  String[] file_type, 
                                  String[] encryption_key, 
                                  String[] status, 
                                  String[] group_name, 
                                  String[] type, 
                                  String[] target) { // batch!
        int result = 0;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            try {
                ps = conn.prepareStatement(this.insert_ticket);
                
                for (int c = 0; c<account_stable_id.length; c++) {
                    ps.setString(1, account_stable_id[c]);
                    ps.setString(2, download_ticket[c]);
                    ps.setString(3, session_token[c]);
                    ps.setString(4, client_ip[c]);
                    ps.setString(5, file_stable_id[c]);
                    ps.setString(6, file_type[c]);
                    ps.setString(7, encryption_key[c]);
                    ps.setString(8, status[c]);
                    ps.setString(9, group_name[c]);
                    ps.setString(10, type[c]);
                    ps.setString(11, target[c]);
                    ps.addBatch();
                    
                    result++;

                    // Execute query (every 1000, and last one)
                    if (result%1000 == 0 || c==(account_stable_id.length-1))
                        ps.executeBatch();
                }
                
            } catch (SQLException ex) {
                System.out.println("::: Error: " + ex.toString());
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    public void deleteTicket(String download_ticket, String client_ip) {
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            try {
                ps = conn.prepareStatement(this.delete_ticket);
                ps.setString(1, download_ticket);
                ps.setString(2, client_ip);

                // Execute query
                ps.executeUpdate();
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(ps);
            }
        }
    }
    
    public void deleteRequest(String request, String client_ip) {
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            try {
                ps = conn.prepareStatement(this.delete_request);
                ps.setString(1, request);
                ps.setString(2, client_ip);

                // Execute query
                ps.executeUpdate();
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(ps);
            }
        }
    }
    
    public void localizeRequest(String email, String request, String client_ip) {
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            try {
                ps = conn.prepareStatement(this.localize_request);
                ps.setString(1, request);
                ps.setString(2, client_ip);

                // Execute query
                ps.executeUpdate();
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(ps);
            }
        }
    }
    
    public String[] getDatasetsByUser(String email, String DAC) {
        String[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<String> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.dataset_users);
                ps.setString(1, email);
                ps.setString(2, DAC);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results                
                while (rs.next())
                    resultset.add(rs.getString(1));
                
                // Place result in Array
                result = resultset.toArray(new String[resultset.size()]);               
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    public String[] getUsersByDatasetDac(String dataset, String DAC) {
        String[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<String> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.user_datasets_dac);
                ps.setString(1, dataset);
                ps.setString(2, DAC);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results                
                while (rs.next())
                    resultset.add(rs.getString(1));
                
                // Place result in Array
                result = resultset.toArray(new String[resultset.size()]);               
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }

    public EgaPermission[] getUsersByDataset(String dataset) {
        EgaPermission[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<EgaPermission> resultset = new ArrayList<>();
                
                String user, status, date, changed;

                ps = conn.prepareStatement(this.user_dataset);
                ps.setString(1, dataset);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results                
                while (rs.next()) {
                    user = rs.getString(1);
                    status = rs.getString(2); // 'approved' or 'pending' or 'revoked'
                    date = rs.getString(3); // last updated
                    changed = rs.getString(4); // e.g. 'ega'
                    
                    resultset.add(new EgaPermission(user, status, date, changed));
                }
                
                // Place result in Array
                result = resultset.toArray(new EgaPermission[resultset.size()]);               
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    public String[] getRequestsLight(String email) {
        String[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<String> resultset = new ArrayList<>();

                ps = conn.prepareStatement(this.requests_light);
                ps.setString(1, email);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results                
                while (rs.next())
                    resultset.add(rs.getString(1) + "\t" + rs.getString(2));
                
                // Place result in Array
                result = resultset.toArray(new String[resultset.size()]);               
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    // -------------------------------------------------------------------------
    // --- DB Access Functions -------------------------------------------------
    // -------------------------------------------------------------------------

    private Connection createConnection() {
        Connection connection = null;
        try {
            connection = this.dbSource.getConnection();
            connection.setAutoCommit(true);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }

        return connection;
    }    
    
    private Connection createConnection(DataSource dataSource) throws SQLException {
        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(true);

        return connection;
    }    
    
    // -------------------------------------------------------------------------
    // --- Close connections without considering error messages ----------------
    // -------------------------------------------------------------------------

    private void closeQuitely(Statement stmt) {
        if(stmt != null) {
	    Connection con = null;
	    try {
		con = stmt.getConnection();
	    } catch (SQLException e) {
	    }
	    try {
                stmt.close();
            } catch (SQLException e) {
                // ignore
            }
	    closeQuitely(con);
        }
    }

    private void closeQuitely(ResultSet rs) {
        if(rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private void closeQuitely(Connection con) {
	if (con != null) {
	    try {
		con.close();
	    } catch (SQLException e) {
		// ignore
	    }
	}
    }

    
    // -------------------------------------------------------------------------
    // --- Additional Functionality --------------------------------------------
    // -------------------------------------------------------------------------
    
    // Query the Fire Metadata Server for actual File Path/URL, given a relative path
    private String[] getPath(String path) {
        if (path.equalsIgnoreCase("Virtual File")) return new String[]{"Virtual File"};
        
        try {
            ArrayList<String> temp_path = new ArrayList<>();
            String[] result = new String[4]; // [0] name [1] stable_id [2] size [3] rel path
            result[0] = "";
            result[1] = "";
            result[3] = path;
            String path_ = path;

            // Sending Request
            HttpURLConnection connection = null;
            connection = (HttpURLConnection)(new URL("http://oy-fire-meta.ebi.ac.uk/fire/direct")).openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("X-FIRE-Archive", "ega");
            connection.setRequestProperty("X-FIRE-Key", "6c5ea1fd211d7571a916245b76ebcd8c469fd8c2");
            connection.setRequestProperty("X-FIRE-FilePath", path_);

            // Reading Response
            int responseCode = connection.getResponseCode();

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();

            ArrayList<String[]> paths = new ArrayList<>();
            
            String location_http = "", 
                   location_http_tag = "", 
                   location_md5 = "";
            while ((inputLine = in.readLine()) != null) {
                if (inputLine.startsWith("FILE_PATH"))
                    temp_path.add(inputLine.substring(inputLine.indexOf("/")).trim());
                if (inputLine.startsWith("HTTP_GET"))
                    location_http = inputLine.substring(inputLine.indexOf("http://")).trim();
                if (inputLine.startsWith("AUTH_BASIC"))
                    location_http_tag = inputLine.substring(inputLine.indexOf(" ")+1).trim();
                if (inputLine.startsWith("FILE_MD5")) {
                    location_md5 = inputLine.substring(inputLine.indexOf(" ")+1).trim();
                    paths.add(new String[]{location_http, location_http_tag, location_md5});
                }
            }
            in.close();

            if (paths.size() > 0) {
                for (int i=0; i<paths.size(); i++) {
                    String[] e = paths.get(i);
                    if (e[1].contains("egaread")) {
                        result[0] = e[0];
                        result[1] = e[1];
                        result[2] = String.valueOf(getLength(new String[]{location_http, location_http_tag}));
                    }
                }
            } else if (temp_path.size()>0 && result[1].length()==0) { // Determine proper path
                for (String temp_path1 : temp_path) {
                    if ((new File(temp_path1)).exists()) {
                        result[0] = temp_path1;
                        result[2] = String.valueOf((new File(temp_path1)).length());
                        break;
                    }
                }
                result[1] = "";
            }
            
            return result;
        } catch (Exception e) {
            System.out.println("Path = " + path);
            System.out.println(e.getMessage());
            //e.printStackTrace();
        }            
        
        return null;
    }

    // Get the length of a file, from disk or Cleversafe server
    private long getLength(String[] path) {
        long result = -1;
        
        try {
            if (path[1] != null && path[1].length() == 0) { // Get size of file directly
                File f = new File(path[0]);
                result = f.length();
            } else { // Get file size from HTTP
                // Sending Request
                HttpURLConnection connection = null;
                connection = (HttpURLConnection)(new URL(path[0])).openConnection();
                connection.setRequestMethod("HEAD");

                String userpass = path[1];
                
                // Java bug : http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6459815
                String encoding = new sun.misc.BASE64Encoder().encode (userpass.getBytes());
                encoding = encoding.replaceAll("\n", "");  
                
                String basicAuth = "Basic " + encoding;
                connection.setRequestProperty ("Authorization", basicAuth);
                
                // Reading Response
                int responseCode = connection.getResponseCode();

                String headerField = connection.getHeaderField("content-length");
                String temp = headerField.trim();
                result = Long.parseLong(temp);

                connection.disconnect();
            }
        } catch (IOException | NumberFormatException e) {
            e.printStackTrace();
        }            
        
        return result;
    }
    
}
