/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ColumnDefinitions;


import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.nio.ByteBuffer;

/**
 * Cassandra 2.0.8 client for YCSB framework
 */
public class CassandraClient20 extends DB {

    private String host = "";
    private String keyspace = "";
    private String tableName = "";
    
    private Cluster cluster;
    private Session session;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        host = getProperties().getProperty("cassandra.host", "localhost");  //entry point to the cassandra cluster ie. 172.168.12.1
        keyspace = getProperties().getProperty("cassandra.keyspace", "ycsb");
        tableName = getProperties().getProperty("cassandra.tablename", "usertable");
        
        connectToDatabase(host, keyspace);       
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one DB
     * instance per client thread.
     */
    @Override 
    public void cleanup() throws DBException {
        //closeDatabase();
    }

    /**
     * Read a record from the database. Each field/value pair from the result will
     * be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    @Override 
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        Select.Selection selection = QueryBuilder.select();

        if (fields == null || fields.isEmpty()) {
            selection.all();
        } else {
            for (String field : fields) {
                selection.column(field);
            }      
        }
        
        Select select = selection.from(keyspace, tableName);
        select.where(QueryBuilder.eq("ycsb_key", key));

        try {                      
            ResultSet results = session.execute(select);
            Row row = results.one();
            if (row != null) {
                for (ColumnDefinitions.Definition column: results.getColumnDefinitions() ) {
                    result.put(column.getName(), new StringByteIterator(row.getString(column.getName())));
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to read data from database: " + e.toString());
            System.err.println("  Statement attempted: " + select.toString());
            e.printStackTrace();
            return 1;
        }
        
        return 0;
    }

    /**
    * Perform a range scan for a set of records in the database. Each field/value
    * pair from the result will be stored in a HashMap.
    *
    * @param table The name of the table
    * @param startkey The record key of the first record to read.
    * @param recordcount The number of records to read
    * @param fields The list of fields to read, or null for all of them
    * @param result A Vector of HashMaps, where each HashMap is a set field/value
    *          pairs for one record
    * @return Zero on success, a non-zero error code on error
    */
    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
        Vector<HashMap<String, ByteIterator>> result)
    {
        Select.Selection selection = QueryBuilder.select();

        if (fields == null || fields.isEmpty()) {
            selection.all();
        } else {
            for (String field : fields) {
                selection.column(field);
            }      
        }
        
        Select select = selection.from(keyspace, tableName);
        select.where(QueryBuilder.gt("ycsb_key", startkey));
        select.limit(recordcount);
        
        try {                      
            ResultSet results = session.execute(select);
            for (Row row: results) {
                for (ColumnDefinitions.Definition column: results.getColumnDefinitions() ) {
                    HashMap<String, ByteIterator> record = new HashMap<String, ByteIterator>();
                    record.put(column.getName(), new StringByteIterator(row.getString(column.getName())));
                    result.add(record);
                }
            }            
        } catch (Exception e) {
            System.err.println("Failed to scan data from database: " + e.toString());
            System.err.println("  Statement attempted: " + select.toString());
            e.printStackTrace();
            return 1;
        }
        
        return 0;
    }

    /**
    * Update a record in the database. Any field/value pairs in the specified
    * values HashMap will be written into the record with the specified record
    * key, overwriting any existing values with the same field name.
    *
    * @param table The name of the table
    * @param key The record key of the record to write.
    * @param values A HashMap of field/value pairs to update in the record
    * @return Zero on success, a non-zero error code on error
    */
    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values)
    {
        Update update = QueryBuilder.update(keyspace, tableName);
              
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            String fieldValue = entry.getValue().toString();          
            update.with(QueryBuilder.set(entry.getKey(), fieldValue));
        }
        
        update.where(QueryBuilder.eq("ycsb_key", key));

        try {                      
            session.execute(update);
        } catch (Exception e) {
            System.err.println("Failed to read data from database: " + e.toString());
            System.err.println("  Statement attempted: " + update.toString());
            e.printStackTrace();
            return 1;
        }
        
        return 0;
    }

    /**
    * Insert a record in the database. Any field/value pairs in the specified
    * values HashMap will be written into the record with the specified record
    * key.
    *
    * @param table The name of the table
    * @param key The record key of the record to insert.
    * @param values A HashMap of field/value pairs to insert in the record
    * @return Zero on success, a non-zero error code on error
    */
    @Override 
    public int insert(String table, String key, HashMap<String, ByteIterator> values)
    {    
        Insert insert = QueryBuilder.insertInto(keyspace, table);
        insert.value("ycsb_key", key);
        
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            insert.value(entry.getKey(), entry.getValue().toString());
        }    

        try {                      
            session.execute(insert);
        } catch (Exception e) {
            System.err.println("Failed to insert data into database: " + e.toString());
            System.err.println("  Statement attempted: " + insert.toString());
            e.printStackTrace();
            return 1;
        }
         
        return 0; 
    }

    /**
    * Delete a record from the database.
    *
    * @param table The name of the table
    * @param key The record key of the record to delete.
    * @return Zero on success, a non-zero error code on error
    */
    @Override
    public int delete(String table, String key)
    {
        Delete delete = QueryBuilder.delete(keyspace, table).from(key, "contact");
        delete.where(QueryBuilder.eq("ycsb_key", key));
            
        try {                      
            session.execute(delete);
        } catch (Exception e) {
            System.err.println("Failed to delete data into database: " + e.toString());
            System.err.println("  Statement attempted: " + delete.toString());
            e.printStackTrace();
            return 1;
        }
         
        return 0;         
    }
    
    private void connectToDatabase(String host, String keyspace) {
        cluster = Cluster.builder().addContactPoint(host).build();
        
        try {            
            printClusterMetadata();             
            setupDatabase(cluster);
            session = cluster.connect(keyspace);
        } catch (Exception e) {
            System.err.println("Failed to initialize connection to cluster: " + e.toString());
            e.printStackTrace();
        }
    }
    
    /*
     * Print the out the cluster's list of hosts to assist in troubleshooting and increase
     * visibility to what's going on when YCSB is run.
     */
    private void printClusterMetadata() {
        try {
            Metadata metadata = cluster.getMetadata();
            System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
            for ( Host host : metadata.getAllHosts() ) {
                System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
            }
        } catch (Exception e) {
            System.err.println("Failed to retrieve cluster metadata: " + e.toString());
            e.printStackTrace();
        }
    }
    
    /*
     * Sets up the database if needed.  This creates it's own session because sessions only work with one keyspace
     * and we haven't created a keyspace yet.
     */
    private void setupDatabase(Cluster cluster) {
        Session setupSession = cluster.connect();
        try {
            setupSession.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication " + 
                "= {'class':'SimpleStrategy', 'replication_factor':3};");
            setupSession.execute("CREATE TABLE IF NOT EXISTS " + keyspace+ ".usertable " +
                "(" + 
                "  ycsb_key text PRIMARY KEY, " +
                "  field0 text," +                
                "  field1 text," +
                "  field2 text," +
                "  field3 text," +
                "  field4 text," +
                "  field5 text," +
                "  field6 text," +
                "  field7 text," +
                "  field8 text," +
                "  field9 text," +
                ");  ");                    
        } catch (Exception e) {
            System.err.println("Failed to setup database: " + e.toString());
            e.printStackTrace();        
        } finally {
            setupSession.close();
        }
    }
}
