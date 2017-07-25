package com.bigsea.rest;



import javax.ws.rs.GET;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.FileFilter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.io.File;




/*
 * 			WS
 */
@Path("/ws")
public class Ws extends Utilities {

	
	
	/* 
	 *                  DAGSIM (normal parameters number)
	 */
	
	@GET
	@Path("/dagsim/{value1}/{value2}/{value3}/{value4}/{value5}")
	@Produces(MediaType.TEXT_HTML)
	public Response dagsimCall(
									@PathParam("value1")  String nNodes, 
									@PathParam("value2")  String nCores,
									@PathParam("value3")  String dataset,
									@PathParam("value4")  String method,
									@PathParam("value5")  String appId
                                   ) {
 		
	String msg = "", dagsimPath, resultsPath;
	dagsimPath = readWsConfig("DAGSIM_HOME");
	
	resultsPath = readWsConfig("RESULTS_HOME");
	
	String path = resultsPath + "/" + nNodes + "_" + nCores + "_" + dataset + "_" + method +"/" + appId;
	
	
	if (dagsimPath == null) msg = "Fatal error: DAGSIM_HOME not defined in wsi_config.xml ";
	if (resultsPath == null) msg = "Fatal error: RESULTS_HOME not defined in wsi_config.xml ";
	
	File f = new File(path);
	if (!f.exists())
	{
		// Get the list of all the folders
    
		File [] directories = new File(readWsConfig("RESULTS_HOME")).listFiles(new FileFilter() {
	    @Override
	    public boolean accept(File file) 
	    {
	    	return file.isDirectory();
	    }
	    });
		
		String reply = bestMatch(directories, nNodes, nCores, dataset, method, appId);
	
		nNodes = reply.substring(0, reply.indexOf(" "));
		nCores = reply.substring(nNodes.length()+1, reply.length()-1);
	
	}
	msg = Start(dagsimPath, BuildLUA(resultsPath, nNodes, nCores, dataset, method, appId));
	 
    return Response.status(200).entity(msg).build();
    
  }
	
	/* 
	 *                  DAGSIM (reduced parameters number)
	 */
	
	@GET
	@Path("/dagsimR/{value1}/{value2}/{value3}/{value4}")
	@Produces(MediaType.TEXT_HTML)
	public Response dagsimExtendedCall(
									@PathParam("value1")  String nNodesnCores, 
									@PathParam("value2")  String dataset,
									@PathParam("value3")  String method,
									@PathParam("value4")  String appId
                                   ) {
 		
	String msg = "", dagsimPath, resultsPath;
	dagsimPath = readWsConfig("DAGSIM_HOME");
	resultsPath = readWsConfig("RESULTS_HOME");
	
	if (dagsimPath == null) msg = "Fatal error: DAGSIM_HOME not defined in wsi_config.xml ";
	if (resultsPath == null) msg = "Fatal error: RESULTS_HOME not defined in wsi_cnfig.xml ";
	
	// Get the list of all the folders
	    
	File [] directories = new File(readWsConfig("RESULTS_HOME")).listFiles(new FileFilter() {
    @Override
    public boolean accept(File file) 
    {
    	return file.isDirectory();
    }
    });
    	
	
	String reply = bestMatchProduct(directories, nNodesnCores, dataset, method, appId);
	
	String newNodes = reply.substring(0, reply.indexOf(" "));
	String newCores = reply.substring(newNodes.length()+1, reply.length());
	
	msg = Start(dagsimPath, BuildLUA(resultsPath, newNodes, newCores, dataset, method, appId));
	 
    return Response.status(200).entity(msg).build();
    
  }
	
	/*
	 * 	             OPT_IC
	 */
	
	@GET
	@Path("/resopt/{value1}/{value2}/{value3}")
	@Produces(MediaType.TEXT_PLAIN)
	public Response Resopt(
            @PathParam("value1")  String appId,
            @PathParam("value2")  String datasize,
            @PathParam("value3")  String deadline) 
	{
			
	String msg1;
	String cmd1;
	
	if (readWsConfig("RESOPT_HOME") == null) msg1 = "Fatal error: RESOPT_HOME not defined in wsi_config.xml ";
	
	/*
	String path = readWsConfig("RESOPT_HOME") + "/" + appId + "_" + datasize + "_" + deadline;
	
	File f = new File(path);
	if (!f.exists())
	{
		// Get the list of all the folders
    
		File [] directories = new File(readWsConfig("RESULTS_HOME")).listFiles(new FileFilter() {
	    @Override
	    public boolean accept(File file) 
	    {
	    	return file.isDirectory();
	    }
	    });
		
		String reply = bestMatch(directories, nNodes, nCores, dataset, method, appId);
	
		nNodes = reply.substring(0, reply.indexOf(" "));
		nCores = reply.substring(nNodes.length()+1, reply.length());
	
	}
	*/
	String[] splited = null;
	Connection connection = null;
	ResultSet resultSet = null;
	String sqlStatement = null;
	String result = null;
	
	try
	{
		connection = readDataBase(
				readWsConfig("AppsPropDB_dbName"),
				readWsConfig("AppsPropDB_IP"),
				readWsConfig("AppsPropDB_user"),
				readWsConfig("AppsPropDB_pass")
				);
		
		connection.setAutoCommit(false);
		 sqlStatement = "select num_vm_opt, num_cores_opt from " + readWsConfig("AppsPropDB_dbName") +".OPTIMIZER_CONFIGURATION_TABLE "+
				"where application_id='" +  appId   + "'" +
				   " and dataset_size='" + datasize + "'" +
				" and deadline=" + deadline;
	
		
		resultSet =  query(readWsConfig("AppsPropDB_dbName"), connection, sqlStatement);
		
		
		
		
		if (resultSet.next())
		{
			
			result = resultSet.getDouble("num_vm_opt") + " " + resultSet.getDouble("num_cores_opt");
			return Response.status(200).entity(result).build();
		}
		else
		{
			/* Find all the record matching app_id, datasize */
			/* select * from OPTIMIZER_CONFIGURATION_TABLE ORDER BY ABS(755000 - deadline) limit 2;*/
			sqlStatement = "SELECT * FROM " + readWsConfig("AppsPropDB_dbName") +".OPTIMIZER_CONFIGURATION_TABLE "+
							"WHERE application_id="+"'" + appId + "'" +
							" AND dataset_size='" + datasize + "' ORDER BY ABS(" + deadline + " - deadline) limit 2";
			
			resultSet =  query(readWsConfig("AppsPropDB_dbName"), connection, sqlStatement);
			
			int[] num_cores_opt = new int[2];
			int[] num_vm_opt = new int[2];
			double[] deadlines = new double[2];
			int index = 0;
		
			
			while (resultSet.next())
			{
				deadlines[index] = resultSet.getDouble("deadline");
				num_cores_opt[index] = resultSet.getInt("num_cores_opt");
				num_vm_opt[index] = resultSet.getInt("num_vm_opt");
				index++;
			}
			
			
			
			result = String.valueOf(Interpolation(Double.valueOf(deadline), deadlines[0], deadlines[1], num_cores_opt)) +
					" " + String.valueOf(Interpolation(Double.valueOf(deadline), deadlines[0], deadlines[1], num_vm_opt));
			
					
			
			/* Invoke OPT_IC */
			cmd1 = "cd " + readWsConfig("RESOPT_HOME")+";./script.sh " + 
			" " + appId + " " + datasize + " " + deadline;  
			msg1 = _run(cmd1);
			
			/* Write on DB the new solution */
			splited = msg1.split("\\s+");
			
			sqlStatement = "insert into " + readWsConfig("AppsPropDB_dbName") +
					".OPTIMIZER_CONFIGURATION_TABLE(application_id, dataset_size, deadline, num_cores_opt, num_vm_opt) values (" +
					"'" + appId + "', '" + datasize + "'," + deadline + "," + splited[0] +"," + splited[1] + ")";
			insert(readWsConfig("AppsPropDB_dbName"), connection, sqlStatement);
			connection.commit();
						
		}
		
		close(connection);
	
	}
	catch(Exception e)
	{
		e.printStackTrace();
	}

    return Response.status(200).entity(result).build();
	//return Response.status(200).entity(sqlStatement).build();
  }

	
	/*
	 * 	             STUDENT'S TOOL (REDUCED PARAMETERS)
	 */
	/*
	@GET
	@Path("/resoptR/{value1}/{value2}/{value3}/{value4}/{value5}/{value6}")
	@Produces(MediaType.TEXT_HTML)
	public Response ResoptR(
			@PathParam("value1")  String nNodesnCores, 
            @PathParam("value2")  String dataset,
            @PathParam("value3")  String method,
            @PathParam("value4")  String appId,
            @PathParam("value5")  String logfile,
            @PathParam("value6")  String deadline) 
	{
			
	String msg1;
	String cmd1;
	String nCores = "", nNodes = "";
	
	if (readWsConfig("RESOPT_HOME") == null) msg1 = "Fatal error: RESOPT_HOME not defined in .demo.properties ";
	
	
		// Get the list of all the folders
    
		File [] directories = new File(readWsConfig("RESULTS_HOME")).listFiles(new FileFilter() {
	    @Override
	    public boolean accept(File file) 
	    {
	    	return file.isDirectory();
	    }
	    });
		
		String reply = bestMatchProduct(directories, nNodesnCores, dataset, method, appId);
	
		nNodes = reply.substring(0, reply.indexOf(" "));
		nCores = reply.substring(nNodes.length()+1, reply.length());
	
	
	
		cmd1 = "cd " + readWsConfig("RESOPT_HOME")+";./script.sh " + nNodes + " " + nCores + " " + dataset + " " + method + 
			" " + appId + " " + logfile + " " + deadline;  
		msg1 = _run(cmd1);
	
	
	 
		return Response.status(200).entity(msg1).build();
  }
  */
}