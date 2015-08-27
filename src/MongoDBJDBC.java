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

public class MongoDBJDBC {
	private static final Logger LOG = LoggerFactory.getLogger(MongoDBJDBC.class);
	public static DB getDatabases(){
	    DB db;
	    MongoClient mongo;
		try {
			// Connect to mongodb
	    	LOG.info("Connecting ... ");
			mongo = new MongoClient("localhost", 27017);
		    db = mongo.getDB("DocumentDB_DataSourceTwitter");
		    LOG.info("Succses fully connection databases");
			return db;
		} catch (UnknownHostException e) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			return null;
		}
	}
	
	public static void main( String args[] ) throws IOException{
    	String[] paths_file;
    	String json;
		DB dba;	
		BufferedReader buf;
		File f  				= null;
		String dir 				= "dataall";
		f       				= new File("/media/ahmadluky/Data/twitter-stream/"+dir+"/");
		paths_file   			= f.list();	
    	dba 					= getDatabases();
        for	(String path:paths_file){
    		LOG.info("/media/ahmadluky/Data/twitter-stream/"+dir+"/"+path);
        	String abs_path 		= "/media/ahmadluky/Data/twitter-stream/"+dir+"/"+path;
			try {
				buf = new BufferedReader(new FileReader(abs_path));
	            while ((json  = buf.readLine()) != null) {
	            	LOG.info(json);
	            	DBObject dbObject 			= (DBObject)JSON.parse(json);
	            	DBCollection collection 	= dba.getCollection("DocumentDB_"+dir);
	    			/**** Insert ****/
	    		    LOG.info("create a document to store key and value");
	    			collection.insert(dbObject);
	            }
			} catch (FileNotFoundException e) {
				System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			}
        }
	}
}
