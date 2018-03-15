/*
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
*/

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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/*
 * 			WS
 */
@Path("/ws")
public class Ws extends Utilities {
   // Executor service is needed to queue the calls to OPT_IC
   static final ExecutorService executor = Executors.newSingleThreadExecutor();
   /* 
    *                  DAGSIM (normal parameters number)
    */
   @GET
   @Path("/dagsim/{value1}/{value2}/{value3}/{value4}/{value5}")
   @Produces(MediaType.TEXT_HTML)
   public Response dagsimCall(
         @PathParam("value1")  String nNodes,
         @PathParam("value2")  String nCores,
         @PathParam("value3")  String ramGB,
         @PathParam("value4")  String datasetSize,
         @PathParam("value5")  String appId
         ) {

	   String msg;
	   
	  /* Read dagSim location and logs folder from $HOME/wsi_config.xml */
      String resultsPath = readWsConfig("RESULTS_HOME");
      String dagsimPath = readWsConfig("DAGSIM_HOME");

      /* Check that the variables have been initialized or raise exception */
      checkConfigurationParameter(resultsPath, "Fatal error: DAGSIM_HOME not defined in wsi_config.xml ");
      checkConfigurationParameter(dagsimPath,  "Fatal error: RESULTS_HOME not defined in wsi_config.xml ");
      
      /* Build the folder path */
      String path = resultsPath + "/" + nNodes + "_" + nCores + "_" + ramGB + "_" + datasetSize +"/" + appId;
      
      /* Find the folder */
      File f = new File(path);
      
      /* If the folder doesn't exist, find the closest match among all the folders available */
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

         checkFoldersExistence(directories, "Fatal error: no sub-directories have been found in " + readWsConfig("RESULTS_HOME"));
        
         /* Find the closest folder to the input one */
         String reply = bestMatch(directories, nNodes, nCores, ramGB, datasetSize, appId);

         /* Update the number of cores and nodes to the the found folder */
         nNodes = reply.substring(0, reply.indexOf(" "));
         nCores = reply.substring(nNodes.length()+1, reply.length()-1);

      }
      
      
      String totalNcores = String.valueOf(Integer.valueOf(nCores) * Integer.valueOf(nNodes));
      try {
         Connection connection = readDataBase(
               readWsConfig("DB_dbName"),
               readWsConfig("DB_IP"),
               readWsConfig("DB_port"),
               readWsConfig("DB_user"),
               readWsConfig("DB_pass")
               );
         connection.setAutoCommit(false);
         String dbName = readWsConfig("DB_dbName");
         //ResultSet lookup_total_time = lookupDagsimStageRemainingTime(connection, dbName, appId, totalNcores, "0", datasetSize);
         ResultSet lookup_total_time = lookupDagsimStageRemainingTime(connection, dbName, appId, totalNcores, "0", datasetSize);
         if (lookup_total_time == null || !lookup_total_time.next()) {
            msg = Start(dagsimPath, BuildLUA(resultsPath, nNodes, nCores, ramGB, datasetSize, appId), connection, dbName, appId, totalNcores, datasetSize);
         }
         else {
            msg = String.valueOf((long)(lookup_total_time.getDouble("val")));
         }
      }
      catch (Exception e) {
         e.printStackTrace();
         return Response.status(500).build();
      }

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
         @PathParam("value2")  String ramGB,
         @PathParam("value3")  String datasetSize,
         @PathParam("value4")  String appId
         ) {

      String msg = "";

      /* Read dagSim location and logs folder from $HOME/wsi_config.xml */
      String resultsPath = readWsConfig("RESULTS_HOME");
      String dagsimPath = readWsConfig("DAGSIM_HOME");

      /* Check that the variables have been initialized or raise exception */
      checkConfigurationParameter(resultsPath, "Fatal error: DAGSIM_HOME not defined in wsi_config.xml ");
      checkConfigurationParameter(dagsimPath,  "Fatal error: RESULTS_HOME not defined in wsi_config.xml ");

      // Get the list of all the folders

      File [] directories = new File(readWsConfig("RESULTS_HOME")).listFiles(new FileFilter() {
         @Override
         public boolean accept(File file)
         {
            return file.isDirectory();
         }
      });


      String reply = bestMatchProduct(directories, nNodesnCores, ramGB, datasetSize, appId);

      String newNodes = reply.substring(0, reply.indexOf(" "));
      String newCores = reply.substring(newNodes.length()+1, reply.length());
      String totalNewNcores = String.valueOf(Integer.valueOf(newCores) * Integer.valueOf(newNodes));
      try {
         Connection connection = readDataBase(
               readWsConfig("DB_dbName"),
               readWsConfig("DB_IP"),
               readWsConfig("DB_port"),
               readWsConfig("DB_user"),
               readWsConfig("DB_pass")
               );
         connection.setAutoCommit(false);
         String dbName = readWsConfig("DB_dbName");
         msg = Start(dagsimPath, BuildLUA(resultsPath, newNodes, newCores, ramGB, datasetSize, appId), connection, dbName, appId, totalNewNcores, datasetSize);
      }
      catch (Exception e) {
         e.printStackTrace();
         return Response.status(500).build();
      }

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
         @PathParam("value2")  String datasetSize,
         @PathParam("value3")  String deadline) 
         {

            if (readWsConfig("RESOPT_HOME") == null) 
               return Response.status(500).entity("Fatal error: RESOPT_HOME not defined in wsi_config.xml ").build();

            Connection connection = null;
            String result = "";

            try
            {
               connection = readDataBase(
                     readWsConfig("DB_dbName"),
                     readWsConfig("DB_IP"),
                     readWsConfig("DB_port"),
                     readWsConfig("DB_user"),
                     readWsConfig("DB_pass")
                     );

               connection.setAutoCommit(false);

               // Create ConfigApp.txt	
               createConfigAppFile(connection, appId, deadline);

               result = callResopt(connection, appId, datasetSize, deadline);

               close(connection);
            }
            catch(Exception e)
            {
               e.printStackTrace();System.exit(-1);
            }

            return Response.status(200).entity(result).build();
         }



   /*
      DAGSIM CALL with stages
      */



   @GET
   @Path("/dagsim/{nNodes}/{nCores}/{ramGB}/{datasetSize}/{appSessId}/{stage}/{currentTimeStamp}")
   @Produces(MediaType.TEXT_PLAIN)
   public Response dagsimCallStages(
         @PathParam("nNodes")  String nNodes,
         @PathParam("nCores")  String nCores,
         @PathParam("ramGB")  String ramGB,
         @PathParam("datasetSize")  String datasetSize,
         @PathParam("appSessId")  String appSessId,
         @PathParam("stage")  String stage,
         @PathParam("currentTimeStamp") String currentTimeStamp) {
         String dagsimPath = readWsConfig("DAGSIM_HOME");
         String resultsPath = readWsConfig("RESULTS_HOME");

         if (dagsimPath == null)
            return Response.status(500)
               .entity("Fatal error: DAGSIM_HOME not defined in wsi_config.xml ")
               .build();

         if (resultsPath == null)
            return Response.status(500)
               .entity("Fatal error: RESULTS_HOME not defined in wsi_config.xml ")
               .build();

         // try connection with database first to retrieve application_id and submission time
         String dbName = readWsConfig("DB_dbName");

         String appId;
         Timestamp submissionTime;
         try {
            Connection connection = readDataBase(
                  readWsConfig("DB_dbName"),
                  readWsConfig("DB_IP"),
                  readWsConfig("DB_port"),
                  readWsConfig("DB_user"),
                  readWsConfig("DB_pass")
                  );

            connection.setAutoCommit(false);

            appId = retrieveAppId(appSessId, dbName, connection);
            submissionTime = retrieveSubmissionTime(appSessId, dbName, connection);

            assert (appId != null && submissionTime != null);

            // Find best match for nNodes and nCores

            String nNodesnCores = findBestMatch(resultsPath, nNodes, nCores, ramGB, datasetSize, appId);
            if (nNodesnCores == null) {
               // Could not find results folder
               return Response.status(500).build();
            }
            nNodes = nNodesnCores.substring(0, nNodesnCores.indexOf(" "));
            nCores = nNodesnCores.substring(nNodes.length()+1, nNodesnCores.length()-1);

            // Check lookup table
            long remainingTime, stage_end_time;

            String totalNcores = String.valueOf(Integer.valueOf(nCores) * Integer.valueOf(nNodes));
            ResultSet lookup_remaining_time = lookupDagsimStageRemainingTime(connection, dbName, appId, totalNcores, stage, datasetSize);
            ResultSet lookup_stage_end_time = lookupDagsimStageEndTime(connection, dbName, appId, totalNcores, stage, datasetSize);


            //if (lookup_result == null)
            if (lookup_remaining_time == null || !lookup_remaining_time.next() || lookup_stage_end_time == null || !lookup_stage_end_time.next())
            {
               // No previous run available, start dagsim
               String dagsimOutput = StartDagsimStages(dagsimPath, BuildLUA(resultsPath, nNodes, nCores, ramGB, datasetSize, appId), connection, dbName, appId, totalNcores, datasetSize);

               String[] stages = getAllStages(dagsimOutput);
               boolean found = false;
               for (String s : stages) {
                  if (s.equals(stage))
                     found = true;
               }
               if (found == false)
                  return Response.status(200).entity("Stage " + stage + " does not exist").build();

               remainingTime = getRemainingTime(dagsimOutput, stage);
               stage_end_time = getStageWaitTime(dagsimOutput, stage);

            }
            else {
               stage_end_time = (long)(lookup_stage_end_time.getDouble("val"));
               remainingTime = (long)(lookup_remaining_time.getDouble("val"));
            }

            Timestamp now = new Timestamp(Long.parseLong(currentTimeStamp));
            long elapsedTime = now.getTime() - submissionTime.getTime();

            long rescaledRemainingTime = Math.round(remainingTime * (float)elapsedTime / stage_end_time);

            return Response.status(200).entity(remainingTime + " " + rescaledRemainingTime).build();            
         }
         catch (Exception e) {
            e.printStackTrace();
            return Response.status(500).build();
         }
         }

   /*
    *                  RESOPT WITH STAGES
    */

   @GET
   @Path("/resopt/{appSessId}/{datasetSize}/{deadline}/{stage}/{currentTimeStamp}")
   @Produces(MediaType.TEXT_PLAIN)
   public Response ResoptStages(
         @PathParam("appSessId")        String appSessId,
         @PathParam("datasetSize")      String datasetSize,
         @PathParam("deadline")         String deadline,
         @PathParam("stage")            String stage,
         @PathParam("currentTimeStamp") String currentTimeStamp)
         {

            String resoptHome = readWsConfig("RESOPT_HOME");
            if (resoptHome == null) {
               return Response.status(500).entity("Fatal error: RESOPT_HOME not defined in wsi_config.xml").build();
            }


            Connection connection = null;
            String result = "";

            String appId;
            Timestamp submissionTime;
            try {
               connection = readDataBase(
                     readWsConfig("DB_dbName"),
                     readWsConfig("DB_IP"),
                     readWsConfig("DB_port"),
                     readWsConfig("DB_user"),
                     readWsConfig("DB_pass")
                     );

               connection.setAutoCommit(false);

               String dbName = readWsConfig("DB_dbName");
               appId = retrieveAppId(appSessId, dbName, connection);
               submissionTime = retrieveSubmissionTime(appSessId, dbName, connection);

               long stageEndTime = getStageEndTimeRunningJob(connection, dbName, appSessId, datasetSize, stage);

               Timestamp now = new Timestamp(Long.parseLong(currentTimeStamp));
               long elapsedTime = now.getTime() - submissionTime.getTime();


               long rescaledDeadline = Math.round((Long.parseLong(deadline)) * ((float) stageEndTime / elapsedTime));

               if (elapsedTime >= Long.parseLong(deadline) || resoptCallInvalid(connection, dbName, appId, datasetSize, rescaledDeadline))
                  return Response.status(200).entity("Deadline too strict").build();

               rescaledDeadline = roundToThousands(rescaledDeadline);
               System.out.println("rescaledDeadline: " + rescaledDeadline + " = " + deadline + " * (" + stageEndTime + " / " + elapsedTime + ")");

               // Call OPT_IC with the rescaled deadline asynchronously
               result = callResopt(connection, appId, datasetSize, String.valueOf(rescaledDeadline));

               int newNumCores = Integer.parseInt(result.split(" ")[0]);
               int newNumVM = Integer.parseInt(result.split(" ")[1]);

               if (newNumCores <= 0 || newNumVM <= 0) {
                  // If the interpolation result is non positive, then return the current number of cores
                  int nCoresRunning = Integer.parseInt(retrieveNcoresRunningJob(connection, dbName, appSessId));
                  newNumCores = nCoresRunning;
                  newNumVM = (int)Math.ceil(nCoresRunning/4.0);
                  result = String.valueOf(newNumCores) + " " + String.valueOf(newNumVM);
               }

               // Update RUNNING_APPLICATION_TABLE with the new value for num_cores
               //updateNumCoresRunningApplication(connection, dbName, appSessId, String.valueOf(newNumCores));

               result = result + " " + rescaledDeadline;
               close(connection);

            }
            catch(Exception e)
            {
               e.printStackTrace();
               return Response.status(500).build();
            }

            return Response.status(200).entity(result).build();
         }


   

   /**
    * First check the cache in the database to see if there is a previously computed value of dagsim -s. If no such value is present, then 
    * start dagsim with option -s, saves data in the database and return stage end time.
    * @param connection
    * @param dbName
    * @param appSessId
    * @param datasetSize
    * @return estimated end time of the stage running with the number of cores specified in RUNNING_APPLICATION_TABLE.
    */
   private long getStageEndTimeRunningJob(Connection connection, String dbName, String appSessId, String datasetSize, String stage) throws SQLException {
      String nCoresRunning = retrieveNcoresRunningJob(connection, dbName, appSessId);
      String appId = retrieveAppId(appSessId, dbName, connection);


      ResultSet lookupDagsim = lookupDagsimStageEndTime(connection, dbName, appId, nCoresRunning, stage, datasetSize);
      if (lookupDagsim != null && lookupDagsim.next()) {
         long stageEndTime = (long)lookupDagsim.getDouble("val");
         return stageEndTime;
      }
      else {
         // No previous run available, start dagsim
         String dagsimPath = readWsConfig("DAGSIM_HOME");
         String resultsPath = readWsConfig("RESULTS_HOME");

         File [] directories = new File(readWsConfig("RESULTS_HOME")).listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) 
            {
               return file.isDirectory();
            }
         });

         String reply = bestMatchProduct(directories, nCoresRunning, datasetSize, "NA", appId);
         System.out.println("bestMatchProduct of " + nCoresRunning + " is " + reply);
         String newNodes = reply.substring(0, reply.indexOf(" "));
         String newCores = reply.substring(newNodes.length()+1, reply.length());

         String luaPath = BuildLUAWithoutMethod(resultsPath, newNodes, newCores, datasetSize, appId);
         String totalNewNcores = String.valueOf(Integer.valueOf(newNodes) * Integer.valueOf(newCores));

         String dagsimOutput = StartDagsimStages(dagsimPath, luaPath, connection, dbName, appId, totalNewNcores, datasetSize);

         long stage_end_time = getStageWaitTime(dagsimOutput, stage);

         return stage_end_time;
      }
   }



   /**
    * Search in the table RUNNING_APPLICATION_TABLE for the number of cores with which the application is running
    * @param connection
    * @param dbName
    * @param appSessId
    * @return the actual number of cores for the application
    * @throws SQLException
    */
   private String retrieveNcoresRunningJob(Connection connection, String dbName, String appSessId) throws SQLException {
      String sqlStatement = "SELECT num_cores FROM " + dbName + ".RUNNING_APPLICATION_TABLE WHERE application_session_id = '" + appSessId + "'";

      ResultSet result = query(dbName, connection, sqlStatement);
      if (result != null && result.next())
         return result.getString("num_cores");
      else
         throw new SQLException("Could not retrieve num cores from database");
   }

   /**
    * Call resopt with the specified parameters. If a previous result is present in cache, then this will be returned.
    * Otherwise, a first approximation will be returned by interpolating the two closest matches and a background call to resopt will be issued.
    * Later calls will then have the computed value of resopt.
    * @param connection
    * @param appId
    * @param datasetSize
    * @param deadline
    * @return A space-separated string of type "<num_cores_opt> <num_vm_opt>" containing the result of resopt.
    */
   public String callResopt(Connection connection, String appId, String datasetSize, String deadline) {
      String sqlStatement = "select num_vm_opt, num_cores_opt from " + readWsConfig("DB_dbName") +".OPTIMIZER_CONFIGURATION_TABLE "+
         "where application_id='" +  appId   + "'" +
         " and dataset_size='" + datasetSize + "'" +
         " and deadline=" + deadline;


      ResultSet resultSet =  query(readWsConfig("DB_dbName"), connection, sqlStatement);
      String result = "";

      try {
         /* Check if the desired configuration already exists in the DB */
         if (resultSet.next())
         {
            result = resultSet.getInt("num_cores_opt") + " "+ resultSet.getInt("num_vm_opt");
         }
         else
         {
            Future<String> future = executor.submit(new ResoptCallable(appId, datasetSize, deadline));

            int[] num_cores_opt = new int[2];
            int[] num_vm_opt = new int[2];
            double[] deadlines = new double[2];

            String leftSqlStatement = "SELECT * FROM " + readWsConfig("DB_dbName") +".OPTIMIZER_CONFIGURATION_TABLE WHERE application_id="+"'" + appId + "' AND dataset_size='" + datasetSize + "' AND num_cores_opt <> '0' AND deadline < " + deadline  +" ORDER BY (" + deadline + " - deadline) limit 1";
            String rightSqlStatement = "SELECT * FROM " + readWsConfig("DB_dbName") +".OPTIMIZER_CONFIGURATION_TABLE WHERE application_id="+"'" + appId + "' AND dataset_size='" + datasetSize + "' AND num_cores_opt <> '0' AND deadline > " + deadline + " ORDER BY (deadline - " + deadline + ") limit 1";

            ResultSet leftResultSet =  query(readWsConfig("DB_dbName"), connection, leftSqlStatement);
            ResultSet rightResultSet =  query(readWsConfig("DB_dbName"), connection, rightSqlStatement);

            ///Interpolation
            if(leftResultSet != null && leftResultSet.next() && rightResultSet != null && rightResultSet.next())
            {
               deadlines[0] = leftResultSet.getDouble("deadline");
               deadlines[1] = rightResultSet.getDouble("deadline");
               num_cores_opt[0] = leftResultSet.getInt("num_cores_opt");
               num_cores_opt[1] = rightResultSet.getInt("num_cores_opt");
               num_vm_opt[0] = leftResultSet.getInt("num_vm_opt");
               num_vm_opt[1] = rightResultSet.getInt("num_vm_opt");
            }
            ///Extrapolation
            else
            {
               String doubleSqlStatement = "SELECT * FROM " + readWsConfig("DB_dbName") + ".OPTIMIZER_CONFIGURATION_TABLE WHERE application_id=" + "'" + appId + "' AND dataset_size='" + datasetSize + "' AND num_cores_opt <> '0' ORDER BY ABS(deadline - " + deadline + ") limit 2";

               resultSet =  query(readWsConfig("DB_dbName"), connection, doubleSqlStatement);

               int index = 0;
               while (resultSet.next())
               {
                  deadlines[index] = resultSet.getDouble("deadline");
                  num_cores_opt[index] = resultSet.getInt("num_cores_opt");
                  num_vm_opt[index] = resultSet.getInt("num_vm_opt");
                  index++;
               }
            }
            System.out.println("For deadline " + String.valueOf(deadline) + " interpolation starts from " + String.valueOf(deadlines[0]) + " and " + String.valueOf(deadlines[1]));

            int numCores = (int)Math.round(Interpolation(Double.valueOf(deadline), deadlines[0], deadlines[1], num_cores_opt));
            int numVM = (int)Math.round(Interpolation(Double.valueOf(deadline), deadlines[0], deadlines[1], num_vm_opt));

            result = String.valueOf(numCores) + " " + String.valueOf(numVM);
         }
      }
      catch (Exception e) {
         e.printStackTrace();//System.exit(-1);
      }

      return result;
   }

   private long roundToThousands(long rescaledDeadline) {
      return 1000*(long)Math.round((double)rescaledDeadline/1000);
   }

   private void updateNumCoresRunningApplication(Connection connection, String dbName, String appSessId, String newNumCores) {
      String query = "UPDATE " + dbName + ".RUNNING_APPLICATION_TABLE SET num_cores = '" + newNumCores + "' WHERE application_session_id = '" + appSessId + "'";

      try {
         insert(dbName, connection, query);
         connection.commit();
      }
      catch(Exception e) {
         e.printStackTrace();//System.exit(-1);
      }
   }

   /**
    * Check if there is a precomputed value for the rescaled deadline whose value is 0 (hence invalid) 
    * @param connection
    * @param dbName
    * @param application_id
    * @param datasetSize
    * @param rescaledDeadline
    * @return 
    */
   private boolean resoptCallInvalid(Connection connection, String dbName, String application_id, String datasetSize, long rescaledDeadline) {
      boolean ret = false;
      try {
         String sqlStatement = "select max(deadline) as maxDeadline from " + dbName + ".OPTIMIZER_CONFIGURATION_TABLE WHERE application_id = '" + application_id + "' and dataset_size = '" + datasetSize + "' and num_cores_opt = '0'";
         ResultSet result = query(dbName, connection, sqlStatement);
         if (result != null && result.next()) {
            long maxDeadline = Long.parseLong(result.getString("maxDeadline"));
            if (maxDeadline >= rescaledDeadline)
               ret = true;
         }
      }
      catch (Exception e) {
    	  //System.exit(-1);
      }

      return ret;
   }
}

/**
 * This method will launch OPT_IC in a new thread
 * @author andrea
 */
class ResoptCallable extends Utilities implements Callable {
   private final String deadline;
   private final String datasetSize;
   private final String appId;

   public ResoptCallable(String appId, String datasetSize, String deadline) {
      this.appId = appId;
      this.datasetSize = datasetSize;
      this.deadline = deadline;
   }

   @Override
   public String call() {
      String msg1 = "0 0";
      try {
         String cmd1 = "cd " + readWsConfig("RESOPT_HOME")+";./script.sh " + 
            " " + appId + " " + datasetSize + " " + deadline;
         System.out.println("OPT_IC command: " + cmd1); 
         msg1 = _run(cmd1);

         /* Write on DB the new solution */
         String[] splited = msg1.split("\\s+");
         // Save to database only if the returned values are positive.

         String sqlStatement = "insert into " + readWsConfig("DB_dbName") +
            ".OPTIMIZER_CONFIGURATION_TABLE(application_id, dataset_size, deadline, num_cores_opt, num_vm_opt) values (" +
            "'" + appId + "', '" + datasetSize + "'," + deadline + "," + splited[0] +"," + splited[1] + ")";


         Connection connection = readDataBase(
               readWsConfig("DB_dbName"),
               readWsConfig("DB_IP"),
               readWsConfig("DB_port"),
               readWsConfig("DB_user"),
               readWsConfig("DB_pass")
               );

         connection.setAutoCommit(false);
         insert(readWsConfig("DB_dbName"), connection, sqlStatement);
         connection.commit();
      }
      catch (Exception e) {
    	  e.printStackTrace(); 
    	  throw new RuntimeException("Error in call of opt_ic"); 
      }
      return msg1;
   }
}
