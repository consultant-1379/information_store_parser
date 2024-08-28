package com.distocraft.dc5000.etl.information_store_parser;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ericsson.eniq.repository.ETLCServerProperties;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;


public class Utils {
	
public ETLCServerProperties etlcserverprops;
	
	private boolean multiBlade;
	private static Logger log;
	
	public Utils(){
		
	}
	
	public Utils(Logger log) {
		Utils.log = log;
	}
	
	public void createDataFile(String path){
		BufferedWriter bw = null;
		try {
			DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");
			Calendar cal = Calendar.getInstance();
			
			String date = dateFormat.format(cal.getTime());

			File file = new File(path, date+".txt");

			if (!file.exists()) {
				file.createNewFile();
			}

			bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
			bw.write(date);
			
		} catch (IOException e) {
			log.log(Level.WARNING, "Unable to write data file", e);
		} finally{
			try {
				if(bw != null)
					bw.close();
			} catch (IOException e) {
				log.warning("Could not close buffered writer.");
			}
		}
	}
	

	public void loadProperties() throws IOException{
		
		etlcserverprops =  new ETLCServerProperties(System.getProperty(ETLCServerProperties.CONFIG_DIR_PROPERTY_NAME)+"/ETLCServer.properties");
				
		try(Scanner scan = new Scanner(new FileReader(System.getProperty(ETLCServerProperties.CONFIG_DIR_PROPERTY_NAME)+"/service_names"))){	
			while(scan.hasNext()){
				if(scan.next().contains("dwh_reader_2")){
					multiBlade = true;
					break;
				}
			}
		}catch (IOException e) {
			log.warning("Could not find the server type. Server will be considered as standalone.");
		}
		
		RockFactory etlrep = null;
		ResultSet result = null;
		
		try{
			log.finest("Multiblade status --- "+multiBlade);
			
			if(multiBlade){
				etlrep = new RockFactory(etlcserverprops.getProperty(ETLCServerProperties.DBURL), etlcserverprops.getProperty(ETLCServerProperties.DBUSERNAME),etlcserverprops.getProperty(ETLCServerProperties.DBPASSWORD), etlcserverprops.getProperty(ETLCServerProperties.DBDRIVERNAME),"InformationStoreParser",false);
				result = executeQuery(etlrep, "select username, connection_string, password, driver_name, connection_name from META_DATABASES where (connection_name = 'dwhrep' or connection_name = 'dwh_reader_2') and type_name = 'USER'");
				loadDbProps(result);
			}else{
				etlrep = new RockFactory(etlcserverprops.getProperty(ETLCServerProperties.DBURL), etlcserverprops.getProperty(ETLCServerProperties.DBUSERNAME),etlcserverprops.getProperty(ETLCServerProperties.DBPASSWORD), etlcserverprops.getProperty(ETLCServerProperties.DBDRIVERNAME),"InformationStoreParser",false);
				result = executeQuery(etlrep, "select username, connection_string, password, driver_name, connection_name from META_DATABASES where (connection_name = 'dwhrep' or connection_name = 'dwh') and type_name = 'USER'");
				loadDbProps(result);
			}
		} catch (SQLException | RockException e) {
			log.warning("Could not get database login details.");
		}finally{
			try {
				if (result != null)
					result.close();
				if (etlrep != null)
					etlrep.getConnection().close();
			} catch (SQLException e) {
				e.printStackTrace();
			}	
		}
	}
	
	public static ResultSet executeQuery(RockFactory dbconn, String sql){
		ResultSet result = null;
		try {
			result = dbconn.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(sql);
		} catch (SQLException e) {
			log.log(Level.WARNING, "Could not retrieve data. "+sql, e);
		}
		return result;
	}
	
	public RockFactory getDBConn(String dbType) throws SQLException, RockException{
			
			if(dbType.contentEquals("dwhdb")){
				return new RockFactory(etlcserverprops.getProperty("dbUrl_dwhdb"), etlcserverprops.getProperty("dwhdb_username"), etlcserverprops.getProperty("dwhdb_password"), etlcserverprops.getProperty("dwhdb_driver"), "InformationStoreParser", false);
			}else if(dbType.contentEquals("repdb")){
				return new RockFactory(etlcserverprops.getProperty("dbUrl_repdb"), etlcserverprops.getProperty("repdb_username"), etlcserverprops.getProperty("repdb_password"), etlcserverprops.getProperty("repdb_driver"), "InformationStoreParser", false);
			}
			return null;
		}
	
	private void loadDbProps(ResultSet result){
		try{
			while(result.next()){
				if(result.getString("connection_name").contentEquals("dwhrep")){
					etlcserverprops.put("repdb_username", result.getString("username"));
					etlcserverprops.put("repdb_password", result.getString("password"));
					etlcserverprops.put("repdb_driver", result.getString("driver_name"));
					etlcserverprops.put("dbUrl_repdb", result.getString("connection_string"));
				}else{
					etlcserverprops.put("dwhdb_username", result.getString("username"));
					etlcserverprops.put("dwhdb_password", result.getString("password"));
					etlcserverprops.put("dwhdb_driver", result.getString("driver_name"));
					etlcserverprops.put("dbUrl_dwhdb", result.getString("connection_string"));
				}
			}
		}catch(Exception e){
			log.warning("Unable to set DB properties.");
		}
		
		log.config(etlcserverprops.getProperty("repdb_username")+" --- "+etlcserverprops.getProperty("repdb_password")+" --- "+etlcserverprops.getProperty("repdb_driver")+" --- "+etlcserverprops.getProperty("dbUrl_repdb"));
		log.config(etlcserverprops.getProperty("dwhdb_username")+" --- "+etlcserverprops.getProperty("dwhdb_password")+" --- "+etlcserverprops.getProperty("dwhdb_driver")+" --- "+etlcserverprops.getProperty("dbUrl_dwhdb"));
	}
	
	public String calculatePreviousRopTime(int timeOffset){
        DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Calendar cal = Calendar.getInstance();
        int minOffset = timeOffset%60;
        int hourOffset = timeOffset/60;
        int minutes = cal.get(Calendar.MINUTE);
        int hours = cal.get(Calendar.HOUR_OF_DAY);
        hours = hours+ hourOffset;
        minutes = minutes - (minutes % 5) +minOffset;
        cal.set(Calendar.MINUTE, minutes);
        cal.set(Calendar.HOUR_OF_DAY, hours);
        cal.clear(Calendar.SECOND);
        return dateTimeFormat.format(cal.getTime());
	}
	
	public HashMap<String, Set<String>> getPartitions(RockFactory repdb, List<String> queries) throws SQLException{
		
		HashMap<String,Set<String>> tablePartitions = new HashMap<>();
		
		Pattern p = Pattern.compile("DC_E_.*?_RAW");
		Matcher m = p.matcher(queries.toString());
		
		while(m.find()){
			tablePartitions.put(m.group(), new HashSet<String>());			
		}
		
		String tableNames = tablePartitions.keySet().toString();
		tableNames = tableNames
				.substring(tableNames.indexOf("[")+1, tableNames.indexOf("]"))
				.replace(", ", "', '")
				.replace("_RAW", ":RAW");
		String partitionQuery = "SELECT * FROM DWHPARTITION WHERE CURRENT DATE BETWEEN STARTTIME AND ENDTIME AND STORAGEID in ('"+tableNames+"')";
		ResultSet result = Utils.executeQuery(repdb, partitionQuery);
		
		while(result.next()){
			
			String storageID = result.getString("STORAGEID").replace(":RAW", "_RAW");
			String partition = result.getString("TABLENAME");
			Set<String> partitions;
			if(tablePartitions.containsKey(storageID)){
				partitions = tablePartitions.get(storageID);
				partitions.add(partition);
				
				tablePartitions.put(storageID, partitions);
			}
		}
		
		result.close();
		return tablePartitions;
		
	}
	
	public String modifyQueryWithPartitions(String query, HashMap<String, Set<String>> partitions){
		
		for(Map.Entry<String, Set<String>> entry : partitions.entrySet()){
			String tableView = entry.getKey();
			Object[] tablePartitions = entry.getValue().toArray();
			String partitionQuery = "";
			
			for(int i = 0; i < tablePartitions.length; i++){
				partitionQuery += "SELECT * FROM "+tablePartitions[i];
				for(int j = i; j < tablePartitions.length-1; j++){
					partitionQuery += " UNION ALL ";
				}
			}
			
			query = query.replace(tableView, "("+partitionQuery+")partitions");
		}
		
		return query;
		
	}

	

}
