package com.bigsea.rest;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import java.nio.charset.Charset;
import java.util.Arrays;



import java.io.FileNotFoundException;

import java.util.Enumeration;
import java.util.Properties;

import java.sql.Connection;
import java.sql.DriverManager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Utilities {
	
	
    public ResultSet query(String dbName, Connection connect, String sqlStatement)
    {
    	ResultSet resultSet = null;
    	try
    	{
    		Statement statement = connect.createStatement();

    		resultSet = statement.executeQuery(sqlStatement);
    		
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    	return resultSet; 
    }
    
    public void insert(String dbName, Connection connect, String sqlStatement)
    {
    	
    	try
    	{
    		Statement statement = connect.createStatement();

    		statement.executeUpdate(sqlStatement);
    		
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
 
    }
    
    /* Temporary solution: avg not interpolation */
    public double Interpolation(double x, double x1, double x2, int[] values)
    {
    	double res1;
    	double res2;
    	
    	res1 = (x - x1)/(x2 - x1);
    	res2 = values[1] - values[0];
    	
    	return values[0] + res1*res2;
    }
    
   
	public Connection readDataBase(String dbName, String Ip, String user, String password			
			) throws Exception 
	{
		Connection connect = null;
	 try 
        {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");
            // Setup the connection with the DB
            String conn_string = "jdbc:mysql://" + Ip +"/" + dbName + "?"
                    + "user=" + user + "&password="+ password;
            connect = DriverManager.getConnection(conn_string);             
     
            
        } 
        catch (Exception e) 
        {
            throw e;
        } 
        
	 
	 return connect;
    }
	
	public String writeResultSet(ResultSet resultSet) throws SQLException {
		String application_id = "";
		String dataset_size = "";
		String deadline = "";
		
        while (resultSet.next()) {

            application_id = resultSet.getString("application_id");
            dataset_size = resultSet.getString("dataset_size");
            deadline = resultSet.getString("deadline");

        }
        return application_id +" " + dataset_size + " "+ deadline;
    }
	
	
	public void close(Connection connect) {
        try {
            

            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {

        }
    }
	
	public String bestMatchProduct(File[] directories, String nNodesnCores, String dataset_size, String param, String app_id)
	{
		int min = Integer.MAX_VALUE;
 	    int diff = 0;
 	    int prod;
 	    String parameters = "";
 	    int newnNodes, newnCores;
 	    
 	   for (int i = 0; i < directories.length; i++)
 	   {	
 		   newnNodes = extract(directories[i].getName(), 1);
 		   newnCores = extract(directories[i].getName(), 2);
 		   prod =  newnNodes * newnCores;
 		   diff = Math.abs(Integer.valueOf(nNodesnCores) - prod);
 		  if (min > diff) 
		   {
			   parameters = String.valueOf(newnNodes) + " "+ String.valueOf(newnCores);
			   min = diff;
		   }
 	   }
 	    
 	   return parameters;
	}
	
	public String readWsConfig(String variable)
	{
		String filename = System.getenv("HOME") + "/" + "wsi_config.xml";
			try {
				File file = new File(filename);
				FileInputStream fileInput = new FileInputStream(file);
				Properties properties = new Properties();
				properties.loadFromXML(fileInput);
				fileInput.close();

				Enumeration enuKeys = properties.keys();
				while (enuKeys.hasMoreElements()) 
				{
					String key = (String) enuKeys.nextElement();
					String value = properties.getProperty(key);
					//System.out.println(key + ": " + value);
					if (key.equals(variable)) return(value);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return("Error: variable not found in wsi_cnfig.xml file");
		}
		

	
public String bestMatch(File[] directories, String nNodes, String nCores, String dataset_size, String param, String app_id)
	{

		int min = Integer.MAX_VALUE;
 	    int diff = 0;
 	    int position = 1;
 	     String pattern;
 	     
 	    pattern = nNodes + "_" + nCores + "_" + dataset_size + "_" + param +
         		"/" + app_id;
 	    boolean  nodesFound = false, coresFound = false;
 	    int saveIndex[] = new int[2];
 	    String newNNodes = "", newNCores = "";

 	    String parameters = nNodes + " " + nCores + " ";
		
	       
	    	       
	     // Find the best match. Consider first number of nNodes then nCores
	    for (position = 0; position <2; position++)
	    {	  
	    	min = Integer.MAX_VALUE;
	    	for (int i = 0; i < directories.length; i++)
	    	{	    
	    		
	    		diff =  Math.abs((extract(pattern, position+1) - extract(directories[i].getName(), position+1)));
	    		// diff == 0 means the two parameters match
	    		if (diff == 0) 
	    		{
	    			   if (position+1 == 1) nodesFound = true;
	    			   else if (position+1 == 2) coresFound = true;
	    			   else System.exit(-1);
	    			   break; // pass to the other parameters or quit
	    		}
	    		else
	    			   if (min > diff) 
	    			   {
	    				   saveIndex[position] = i;
	    				   min = diff;
	    			   } 
	    	   }
	    	  }
	    	  
	    	  // Prepare the new parameters
	    	  if (!nodesFound) newNNodes = String.valueOf(extract(directories[saveIndex[0]].getName(), 1));
	    	  if (!coresFound) newNCores = String.valueOf(extract(directories[saveIndex[1]].getName(), 2));
	    	 
	    	   if (coresFound && !nodesFound) // it means a match for nNodes has been found
	    		   	parameters = Integer.valueOf(newNNodes) + " " + Integer.valueOf(nCores) + " ";
	    	   		else // it means a match for nCores has been found
	    	   			if (!coresFound && nodesFound)  parameters = nNodes + " " + newNCores + " "; 
	    	   				else // It means a match for nNodes has been found
	    	   					if (!coresFound && !nodesFound) parameters = newNNodes + " " + newNCores + " ";

	    	   return parameters;
	       }

	
	
	public String Start(String path, String luafilename)
	  {
		  String cmd, errorMsg = new String("ERROR: could not open lua file. Check .demo_properties file");
			cmd = "cd ".concat(path).concat(";").concat("./dagsim.sh ");
			
			cmd = cmd.concat(luafilename).concat(" 2>&1|sed -n 1,1p|awk '{print $3\" \" $4\" \" $5\" \" $6\" \" $7}'");
			//msg = _run("cd /Users/Enrico/Dropbox/Developement/C/DagSim/;./dagSim test1.lua 2>&1|sed -n 1,1p|awk '{print $3\" \" $4\" \" $5\" \" $6\" \" $7}'");
		
			errorMsg = _run(cmd);
			
			
			
			if (errorMsg.contains("ERROR")) return(cmd.concat(" ").concat(errorMsg));//return ("ERROR: could not open lua file. Check .demo_properties file");
			else return errorMsg;
			
			// return ("cmd = ".concat(cmd));
	  }
	
	public int extract(String string, int position)
	{
		String mysubstring = "", savestring = "";

		
		for (int i = 1; i <= position; i++)
		{
			mysubstring = string.substring(0, string.indexOf("_"));
			savestring = mysubstring;
			string = string.substring(mysubstring.length()+1, string.length());	
		}
		
			return Integer.valueOf(mysubstring);
	}
	
	public String uploadCsvToTable(String uploadedFileLocation, String filename)
	{
		String cmd = readVar("OPTIMIZE_HOME")+"/optimize "+uploadedFileLocation + " 100 " + filename;
	       return _run(cmd);
	}
	
	
	
	
	public String readVar(String property)
	  {
		  String line, path="";
			int index;
			try {
			    InputStream fis = new FileInputStream(System.getenv("HOME").concat("/.ws_properties"));
			    InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
			    BufferedReader br = new BufferedReader(isr);
			   
			    while ((line = br.readLine()) != null) {
			        if (line.contains(property))
			        {
			        	index = line.indexOf('=');
			        	path=line.substring(index+1, line.length());
			        	break;
			        }
			    }
			    br.close();
				}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			return(path);
	  }
	  
	  public String BuildLUA(String resultsPath, String iD, String dsDimension, String coreN, String method, String query)
	  {
		  String path = resultsPath.concat("/").concat(iD).concat("_").concat(dsDimension).concat("_").concat(coreN).concat("_").concat(method).concat("/").concat(query).concat("/logs/");
		  
		  String luaFileName = getFirstFile(getFirstFolder(path));
		  
		  return(luaFileName);
	  }
	 
	  
	  
	  public String getFirstFolder(String path)
	  {          
		  String folderName = path;
		  try
		  {
			  File folder = new File(path);
			  File[] listOfFiles = folder.listFiles();
			  int i;
		  
			  Arrays.sort(listOfFiles);
			  for (i = 0; i < listOfFiles.length; i++) 
				  if (listOfFiles[i].isDirectory()) break;
			  folderName = listOfFiles[i].getAbsolutePath();
		  }
		  catch(Exception e)
		  {
			  e.printStackTrace();
			  
		  }
	      
	      return folderName.concat("/");
	  }
	  
	  public String getFirstFile(String path)
	  {          
		  // Initialize to worst case: something is wrong with the filename or the path
		  String filename = "ERROR: could not find file";
		  try
		  {
			  File folder = new File(path);
			  File[] listOfFiles = folder.listFiles();
			  int i;
		  
			  for (i = 0; i < listOfFiles.length; i++) 
				  if (
						  (listOfFiles[i].isFile()) && 
						  (listOfFiles[i].getName().endsWith(".lua"))
						  ) break;
			  filename = listOfFiles[i].getAbsolutePath();
		  }
		  catch(Exception e)
		  {
			  e.printStackTrace();
			  
		  }
	      
	      return filename;
	      
	  }
	
	public String _run(String cmd) {    
	    try
	    {
	    	final ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", cmd);
	 	    pb.redirectErrorStream();
	    	final Process process = pb.start();
	    	final InputStream in = process.getInputStream();
	    	final byte[] bytes = new byte[1024];
	    	final ByteArrayOutputStream baos = new ByteArrayOutputStream();

	    	
	    	for (int len; (len = in.read(bytes)) > 0;)
	        baos.write(bytes, 0, len);
	    	process.waitFor();
	    	return baos.toString(); 
	    	
	    }
	    catch(IOException e)
	    {
	    	e.printStackTrace();
	    }
	    catch(InterruptedException e)
	    {
	    	e.printStackTrace();
	    }
	    return("Error: could not execute command "+cmd);
	}
	
	// save uploaded file to new location
		public void writeToFile(InputStream uploadedInputStream,
				String uploadedFileLocation) {

			try {
				OutputStream out = new FileOutputStream(new File(
						uploadedFileLocation));
				int read = 0;
				byte[] bytes = new byte[1024];

				out = new FileOutputStream(new File(uploadedFileLocation));
				while ((read = uploadedInputStream.read(bytes)) != -1) {
					out.write(bytes, 0, read);
				}
				out.flush();
				out.close();
			} catch (IOException e) {

				e.printStackTrace();
			}

		}

}
