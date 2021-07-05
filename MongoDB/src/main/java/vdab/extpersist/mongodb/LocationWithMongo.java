package vdab.extpersist.mongodb;

import vdab.api.event.VDABData;
import vdab.api.node.JavaFunctionSet_A;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

public class LocationWithMongo extends JavaFunctionSet_A{
	public VDABData func_saveLocation(VDABData vData, String path) {

		String userID = vData.getDataAsString(path+".UserID");
		
		long time = vData.getDataAsLong(path+".Datetime").longValue();
		double lon = vData.getDataAsDouble(path + ".Longitude").doubleValue();
		double lat = vData.getDataAsDouble(path + ".Latitude").doubleValue();
		
		MongoClient mongoClient = new MongoClient( "localhost" , 27017);
		MongoDatabase database = mongoClient.getDatabase("testdb");
		MongoCollection<Document> collection = database.getCollection("test");
		
		Document doc = new Document("name", "record")
	                .append("type", "database")
	                .append("count", 1)
	                .append("Datetime", time)
	                .append("UserID", userID)
	                .append("Longitude", lon)
	                .append("Latitude", lat);
		
		 collection.insertOne(doc);
		 mongoClient.close();

		 return vData;
	}
}
