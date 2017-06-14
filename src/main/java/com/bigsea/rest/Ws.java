package com.bigsea.rest;



import javax.ws.rs.GET;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.FileFilter;


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
	
	
	if (dagsimPath == null) msg = "Fatal error: DAGSIM_HOME not defined in .demo.properties ";
	if (resultsPath == null) msg = "Fatal error: RESULTS_HOME not defined in .demo.properties ";
	
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
	
	if (dagsimPath == null) msg = "Fatal error: DAGSIM_HOME not defined in .demo.properties ";
	if (resultsPath == null) msg = "Fatal error: RESULTS_HOME not defined in .demo.properties ";
	
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
	 * 	             STUDENT'S TOOL (ALL PARAMETERS)
	 */
	
	@GET
	@Path("/resopt/{value1}/{value2}/{value3}/{value4}/{value5}/{value6}/{value7}")
	@Produces(MediaType.TEXT_HTML)
	public Response Resopt(
			@PathParam("value1")  String nNodes, 
            @PathParam("value2")  String nCores,
            @PathParam("value3")  String dataset,
            @PathParam("value4")  String method,
            @PathParam("value5")  String appId,
            @PathParam("value6")  String logfile,
            @PathParam("value7")  String deadline) 
	{
			
	String msg1;
	String cmd1;
	
	if (readWsConfig("RESOPT_HOME") == null) msg1 = "Fatal error: RESOPT_HOME not defined in .demo.properties ";
	
	String path = readWsConfig("RESOPT_HOME") + "/" + nNodes + "_" + nCores + "_" + dataset + "_" + method +"/" + appId;
	
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
	
	cmd1 = "cd " + readWsConfig("RESOPT_HOME")+";./script.sh " + nNodes + " " + nCores + " " + dataset + " " + method + 
			" " + appId + " " + logfile + " " + deadline;  
	msg1 = _run(cmd1);
	
	
	 
    return Response.status(200).entity(msg1).build();
  }

	/*
	 * 	             STUDENT'S TOOL (REDUCED PARAMETERS)
	 */
	
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
}