import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.util.JSON;

/**
 * 
 * @author ahmadluky
 * function for read data from API twitter  insert to MONGODB
 */

public class MongoDBJDBC {
	private static final Logger LOG = LoggerFactory.getLogger(MongoDBJDBC.class);
	public static DB getDatabases(){
	    DB db;
	    MongoClient mongo;
		try {
			// runnig mongoDB : mongod --dbpath /data/mongodb/
			// Connect to mongodb
	    	LOG.info("Connecting ... ");
			mongo = new MongoClient("localhost", 27017);
		    db = mongo.getDB("dataTwitter");			// name databases
		    LOG.info("Succses fully connection databases");
			return db;
		} catch (UnknownHostException e) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			return null;
		}
	}
	
	public static void main( String args[] ) throws IOException
	{	
		String json;
		DB dba 			= getDatabases();
		String dir 		= "rk";											// dir is name tokoh
		File f   		= new File("/home/ahmadluky/twitterdata/"+dir+"/"); // define a paht dataTwitter
		String[] paths	= f.list();	
		
		for (String dirDate:paths)
		{
			LOG.info(dirDate);
			File dDate 				= new File("/home/ahmadluky/twitterdata/"+dir+"/"+dirDate+"/");
			String [] dDateFile 	= dDate.list();
			for	(String path:dDateFile)
	        {
	        	String abs_path = "/home/ahmadluky/twitterdata/"+dir+"/"+dirDate+"/"+path;
	        	LOG.info(abs_path);
				try {
					BufferedReader buf = new BufferedReader(new FileReader(abs_path));
		            while ((json  = buf.readLine()) != null) {
		            	DBObject dbObject = (DBObject)JSON.parse(json);
		            	DBCollection collection = dba.getCollection("data_"+dir+"_"+dirDate);
		    			/**** Insert ****/
		    		    LOG.info("create a document to store key and value");
			    		collection.insert(dbObject);
		            }
		            buf.close();
				} catch (FileNotFoundException e) {
					System.err.println( e.getClass().getName() + ": " + e.getMessage() );
				}
	    		
	        }
		}
		LOG.info("succes !!");
	}
}
