package com.distocraft.dc5000.etl.information_store_parser;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.etl.parser.Main;
import com.distocraft.dc5000.etl.parser.MeasurementFile;
import com.distocraft.dc5000.etl.parser.Parser;
import com.distocraft.dc5000.etl.parser.SourceFile;
import com.distocraft.dc5000.etl.parser.TransformerCache;
import com.ericsson.eniq.repository.ETLCServerProperties;



import ssc.rockfactory.RockFactory;

public class information_store_parser implements Parser{

	private Utils utils;
	private Logger log;
	private String techPack;
    private String setType;
    private String setName;
    private int status = 0;
    private Main mainParserObject = null;
    private String workerName = "";
    private boolean completed = false;
    private RockFactory dwhdb;
    private RockFactory repdb;
    
    private String nodeTypeStatus;
    private String timeOffsetValue;
    private String nodeTypeMeasures;
    private String measureID;
	private String processingOperation;
	private String sourceTables;
	private String targetTable;
	private String targetColumns;
	private String measureSQL;
	private ResultSet measureData;
	private MeasurementFile mFile;
    
    public ETLCServerProperties etlcserverprops;
    
	@Override
	public void run() {
		try {
            this.status = 2;
            SourceFile sf = null;
            completed = false;
                  
            while ((sf = mainParserObject.nextSourceFile()) != null) {

                try {
                    mainParserObject.preParse(sf);
                    if(!completed){
                    	parse(sf, techPack, setType, setName);
                    }
                    mainParserObject.postParse(sf);
                } catch (final Exception e) {
                    mainParserObject.errorParse(e, sf);
                } finally {
                    mainParserObject.finallyParse(sf);
                }
            }
        } catch (final Exception e) {
            log.log(Level.WARNING, "Worker parser failed to exception", e);
        } finally {
            this.status = 3;
        }
		
	}


	@Override
	public void init(final Main main, final String techPack, final String setType, final String setName, final String workerName) {
		this.mainParserObject = main;
        this.techPack = techPack;
        this.setType = setType;
        this.setName = setName;
        this.status = 1;
        this.workerName = workerName;
		
        String logWorkerName = "";
        if (workerName.length() > 0) {
            logWorkerName = "." + workerName;
        }
        
        log = Logger.getLogger("etl." + techPack + "." + setType + "." + setName + ".parser.information_store_parser" + logWorkerName);
		
	}

	@Override
	public void parse(final SourceFile sf, final String techPack, final String setType, final String setName) throws Exception {
		TransformerCache.setCheckTransformations(false);
		completed = true;
		utils = new Utils(log);	
		utils.createDataFile(sf.getDir());
		utils.loadProperties();
		
		try {
			dwhdb = utils.getDBConn("dwhdb");
			repdb = utils.getDBConn("repdb");
			
			log.info("techpack name " + techPack);
			
			String tpArray[] = techPack.split("_");
			String nodeType = tpArray[2];
			String tpArrayOSS[] = techPack.split("-");
			String ossID = tpArrayOSS[1];
		
			String tagID = nodeType;			
			
			mFile = Main.createMeasurementFile(sf, tagID, techPack, setType, setName, workerName, log);
			
			if (checkNodeMeasureStatus(nodeType)) {
				generateQueries(nodeType, ossID, mFile);
			} else {
				log.log(Level.INFO, "Measures not active for node type: " + nodeType);
			}
			
		} catch(Exception e) {
			log.log(Level.SEVERE, "Parser initiation failed", e);
		} finally{
			try{
				if(dwhdb!=null){
					if((dwhdb.getConnection()!=null ) && (!dwhdb.getConnection().isClosed())){
						dwhdb.getConnection().close();
						log.info("Dwhdb connection is closed "+dwhdb.getConnection().isClosed());
					}
				}
			}catch(Exception e){
				log.warning("Exception while closing the dwhdb connection "+e.getMessage());
			}
			try{
				if(repdb!=null){
					if((repdb.getConnection()!=null ) && (!repdb.getConnection().isClosed())){
						repdb.getConnection().close();
						log.info("repdb connection is closed "+repdb.getConnection().isClosed());
					}
				}
			}catch(Exception e){
				log.warning("Exception while closing the repdb connection "+e.getMessage());
			}
		
		
	}
	}
	

	@Override
	public int status() {
        return status;
    }
	
	
	private boolean checkNodeMeasureStatus(String nodeType){
		boolean nodeCheckStatus = false;
		nodeTypeStatus = "SELECT NODE_MEASURE_STATUS, TIME_OFFSET FROM DIM_CV_NODE_CONTROL WHERE NODE_TYPE = '" + nodeType + "'";
		log.log(Level.FINE, "Node Status: " + nodeTypeStatus);
			
		try {
			ResultSet nodeStatus = Utils.executeQuery(dwhdb, nodeTypeStatus);
			while (nodeStatus.next()) {
				if (nodeStatus.getString("NODE_MEASURE_STATUS").toLowerCase().equals("active")) {
					timeOffsetValue = nodeStatus.getString("TIME_OFFSET");
					nodeCheckStatus = true;
				}
			}
			nodeStatus.close();
		} catch (SQLException e) {
			log.log(Level.SEVERE, "Unable to retrieve data" ,e);
		}catch(Exception e) {
			log.log(Level.WARNING, "Unable to return Node measure status"  ,e);
		}
		return nodeCheckStatus;
	}
	
	
	private void generateQueries(String nodeType, String ossID, MeasurementFile mFile) {
		nodeTypeMeasures = "SELECT MEASURE_ID, PROCESSING_OPERATION, SOURCE_TABLES, TARGET_TABLE, "
				+ "TARGET_COLUMNS FROM DIM_CV_MEASURE_CONTROL WHERE NODE_TYPE = '" + nodeType + "' AND UPPER(MEASURE_STATUS) = 'ACTIVE'";
		log.log(Level.FINE, "Node Measure Status: " + nodeTypeMeasures);
		
		try {
			ResultSet nodeMeasures = Utils.executeQuery(dwhdb, nodeTypeMeasures);
			
			List<String> measures = new ArrayList<>();
			List<String> measureIDs = new ArrayList<>();
			
			int timeOffset = Integer.parseInt(timeOffsetValue);
			
			String utcdatetime = utils.calculatePreviousRopTime(timeOffset);
			
			while (nodeMeasures.next()) {
				measureID = nodeMeasures.getString("MEASURE_ID");
				processingOperation = nodeMeasures.getString("PROCESSING_OPERATION");
				sourceTables = nodeMeasures.getString("SOURCE_TABLES");
				targetTable = nodeMeasures.getString("TARGET_TABLE");
				targetColumns = nodeMeasures.getString("TARGET_COLUMNS");

				log.log(Level.FINE, "Measure ID: " + measureID + "\nProcessing Operation: " + processingOperation +
						"\nSource Tables: " + sourceTables + "\ntargetTable: " + targetTable + "\nTarget Columns: " + targetColumns);
				
				String sourceTableArray[] = sourceTables.split(",");
				int sourceLength = sourceTableArray.length;
				
				if (sourceLength > 1) {
					int count = 0;
					measureSQL = "";
					for (String source : sourceTableArray) {
						measureSQL += processingOperation + ", TIMELEVEL, PERIOD_DURATION FROM " + source + " WHERE OSS_ID = '" + ossID + "' AND "
								+ "UTC_DATETIME_ID = '" + utcdatetime + "' AND ROWSTATUS NOT IN ('DUPLICATE','SUSPECTED')";
						count++;
						if(count != sourceLength) {
							measureSQL += " UNION ";
						}
					}
					log.log(Level.FINE, "Measure SQL for Measure ID " + measureID + ": " + measureSQL);
				} else {
					measureSQL = processingOperation + ", TIMELEVEL, PERIOD_DURATION FROM " + sourceTables + " WHERE OSS_ID = '" + ossID + "' AND "
							+ "UTC_DATETIME_ID = '" + utcdatetime + "' AND ROWSTATUS NOT IN ('DUPLICATE','SUSPECTED')";
					
					log.log(Level.FINE, "Measure SQL for Measure ID " + measureID + ": " + measureSQL);
				}
				measures.add(measureSQL);
				measureIDs.add(measureID);
			}
			nodeMeasures.close();
			
			HashMap<String, Set<String>> partitions = utils.getPartitions(repdb, measures);
			if (partitions.isEmpty()) {
				log.log(Level.WARNING, "No data available from Partitions");
			} else {					
				int count = 0;
				for (String measure : measures) {
					measure = utils.modifyQueryWithPartitions(measure, partitions);
					log.log(Level.FINE, "Updated query for partitions: " + measure);
					
					measureData = Utils.executeQuery(dwhdb, measure);
					
					createMeasurementFile(mFile, measureData, targetColumns, measureIDs.get(count));
					
					log.log(Level.INFO, "Measurement file created for Measure ID " + measureIDs.get(count));
					
					count++;
				}
			}
			closeMeasFile(mFile);
			measures.clear();
			measureIDs.clear();
		} catch (SQLException e) {
			log.log(Level.SEVERE, "Unable to retrieve information" ,e);
		} catch(Exception e) {
			log.log(Level.WARNING, "Unable to fetch node measures for " + nodeType + "." ,e);
		}
	}
	
	private void createMeasurementFile(MeasurementFile mFile, ResultSet result, String columns, String measureid) {

		String colArray[] = columns.split(",");
	
		try {
			while(result.next()){
				
				for(String col: colArray){
					mFile.addData(col, result.getString(col));
				}
				mFile.addData("TIMELEVEL", result.getString("TIMELEVEL"));
				mFile.addData("PERIOD_DURATION", result.getString("PERIOD_DURATION"));
				mFile.addData("MEASURE_ID", measureid);
				mFile.addData("DATE_ID", result.getString("DATETIME_ID").split(" ")[0]);
				mFile.saveData();
			
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "Unable to write measurement file for MeasureID" + measureid, e);
		}finally{
			if(result != null){
				try {
					result.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	private void closeMeasFile(MeasurementFile mFile) {
		if (mFile != null){
			try {
				mFile.close();
			} catch (Exception e) {
				log.warning("Unable to close " + mFile.getTagID() + " measurement file.");
				
			}
		}
	}	
	
}
