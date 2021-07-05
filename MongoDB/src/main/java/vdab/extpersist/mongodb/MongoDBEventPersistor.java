package vdab.extpersist.mongodb;


import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;
import org.bson.conversions.Bson;

import vdab.core.persistence.EventPersistor_A;
import vdab.core.persistence.EventUtility;

import com.lcrc.af.AnalysisData;
import com.lcrc.af.AnalysisDataDef;
import com.lcrc.af.AnalysisEvent;
import com.lcrc.af.AnalysisObject;
import com.lcrc.af.constants.GeoUnits;
import com.lcrc.af.datatypes.AFEventDataInfo;
import com.lcrc.af.datatypes.AFEventSearchInfo;
import com.lcrc.af.util.ControlDataBuffer;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;

public class MongoDBEventPersistor extends EventPersistor_A {
	// ADD a ddef for attribute DatabaseName.
	private static AnalysisDataDef[] s_DBForEvent_ddefs = new AnalysisDataDef[]{ 
		AnalysisObject.getClassAttributeDataDef(MongoDBEventPersistor.class,"User")
		.setStandard().setEditOrder(10),
		AnalysisObject.getClassAttributeDataDef(MongoDBEventPersistor.class,"Password")
		.setAsPassword().setStandard().setEditOrder(11),
		AnalysisObject.getClassAttributeDataDef(MongoDBEventPersistor.class,"Server")
		.setRequired().setEditOrder(12),
		AnalysisObject.getClassAttributeDataDef(MongoDBEventPersistor.class,"Port")
		.setRequired().setEditOrder(13),
		AnalysisObject.getClassAttributeDataDef(MongoDBEventPersistor.class,"DatabaseName")
		.setRequired().setEditOrder(14)
	};
	// https://www.mongodb.com/blog/post/getting-started-with-mongodb-and-java-part-i
	private static String COLLECTION_NAME = "EVENT";
	private static String DBNAME = "VDAB";
 
	private String c_DatabaseName = DBNAME;
	private MongoClient c_MongoClient;
	private MongoClientURI c_MongoURI;
	private String c_MongoURIasString;
	private MongoDatabase c_MongoDatabase;
	private MongoCollection c_MongoCollection;
	private Integer c_Port = Integer.valueOf(27017);
	private String c_Server ;
	private String c_User= "";
	private String c_Password= "";

	private static ConcurrentHashMap<String,MongoClient> s_MongoClient_map = new ConcurrentHashMap<String, MongoClient>();

	// ATTRIBUTES ---------------------------------------------------

	public String get_User(){
		return c_User;
	}
	public void set_User( String user){
		c_User = user;
	}

	public String get_Password(){
		return c_Password;
	}
	public void set_Password( String password){
		c_Password = password;
	}

	public String get_Server(){
		return c_Server;
	}
	public void set_Server(String server){
		c_Server = server;
	}
	public Integer get_Port(){
		return c_Port;
	}
	public void set_Port(Integer port){
		c_Port = port;
	}
	
	public String get_DatabaseName(){
		return c_DatabaseName;
	}
	public void set_DatabaseName(String database){
		c_DatabaseName = database;
	}
	public String get_MongoURI(){
		return c_MongoURIasString;
	}
	// ANALYSIS OBJECT Methods
	@Override
	public void _init(){
		initConnection();
		super._init();
	}
	@Override
	public void _reset(){
		initConnection();
	}
	// REQUIRED EventPersistor_A METHODS ---------------------------
	@Override
	public void purgeAllEvents(){
		// Should delete all in the collection
		DeleteResult result = c_MongoCollection.deleteMany(Filters.exists("Path"));
	}
	@Override
	public void purgeEvents(Integer maxAge){
	//TODO - Implement
	// SQL ... DELETE FROM AF_EVENT WHERE SAVETILL < "+System.currentTimeMillis();
		DeleteResult result = c_MongoCollection.deleteMany(Filters.lt("SaveTill" , System.currentTimeMillis()));
		setWarning("result: " + result.toString());
		
	}
	@SuppressWarnings("unchecked")
	@Override
	public AFEventDataInfo[] getAllEventInfo(){
		ArrayList<AFEventDataInfo> l = new ArrayList<AFEventDataInfo>();
		// HACKALERT - Temporary need to get actual minimum and maximum time.
		//	EQUIVALENT SQL "SELECT SOURCE_CONTAINER, PATH, LABEL, MIN(TIMESTAMP),MAX(TIMESTAMP)FROM AF_EVENT GROUP BY SOURCE_CONTAINER, PATH, LABEL");
	
		Document group = new Document("$group",
				new Document("_id",
						new Document("label","$Label")
					.append("path","$Path")
					.append("source","$SourceContainer")
				).append("minTimestamp", new Document("$min","$Timestamp")
				).append("maxTimestamp", new Document("$max","$Timestamp")));
				
		AggregateIterable<Document> documents = c_MongoCollection.aggregate(Arrays.asList(
				
				group
				//Aggregates.group(new Document("_id",new Document("label","$Label").append("path","$Path").append("source","$SourceContainer")))
				
				));
		
		long t1;
		long t0;
		
		String label;
		String path;
		String source;
		
		Document next;
		Document item;
		
		MongoCursor<Document> iterator = documents.iterator();
		while (iterator.hasNext()){
			next = iterator.next();
			item =  (Document) next.get("_id");
			
			label = item.getString("label");
			path = item.getString("path");
			source = item.getString("source");
			
			t0 = next.getLong("minTimestamp").longValue();
			t1 = next.getLong("maxTimestamp").longValue();
			
			System.out.println( "label: "+label+" path: "+path+" source: "+source+" t0: "+t0+" t1: "+t1); 
			
			AFEventDataInfo evInfo = new AFEventDataInfo(source, path, label, t0, t1);
			l.add(evInfo);
		}
		
		
		
		/*
		 * 
		 */
		
		/*
		long t1 = System.currentTimeMillis();
		long t0 = t1 - TimeUnit.DAY*1000L*365L; // Year?
		DistinctIterable<String> sources = c_MongoCollection.distinct("SourceContainer", String.class);
		for (String source: sources){
			DistinctIterable<String> paths = c_MongoCollection.distinct("Path", Filters.eq("SourceContainer", source), String.class);
			for (String path: paths){
				DistinctIterable<String> labels = c_MongoCollection.distinct("Label", Filters.and(Filters.eq("SourceContainer", source),Filters.eq("Path", path)) , String.class);
				for (String label: labels){
					AFEventDataInfo evInfo = new AFEventDataInfo(source, path, label, t0, t1);
					l.add(evInfo);
					}
			}
		}*/
		return l.toArray(new AFEventDataInfo[l.size()]);
	}
	protected AnalysisEvent[] retrieveNearestEvents0(AFEventSearchInfo sinfo){
		ArrayList<AnalysisEvent> l = new ArrayList<AnalysisEvent>();

		Bson retrieveFilter = getBsonFilter(sinfo);
		if (retrieveFilter == null)
			return new AnalysisEvent[0];

		ControlDataBuffer foundPaths = new ControlDataBuffer("DBEventPersistor_foundPaths");

		try { 
			FindIterable<Document> docs = c_MongoCollection.find(retrieveFilter).sort(Sorts.descending("Timestamp") );
			String path = null;
			long leastDif = Long.MAX_VALUE;
			for (Document doc : docs) {
				long nextTime  = doc.getLong("Timestamp").longValue();
				path =doc.getString("Path");	
				String source= doc.getString("SourceContainer");
				String ip = doc.getString("SourceIP");		// REMOTE IP
				String key = getEventKey(path, doc);
				if (!foundPaths.isSet(key)){	
					AnalysisData ad =  EventUtility.retrievePayload(get_PayloadType(), doc.getString("Data"));
					AnalysisEvent ev = new AnalysisEvent(nextTime, path, ad);
					// Set the origin which is always necessary
					ev.setOrigins(source, ip);
					// Set the location if that data is available.
					setLocationForEvent(ev, doc);
					l.add(ev);	
					foundPaths.set(key);
				}
			}
		}
		catch (Exception e){
			setError("Failed to retrieve events e>"+e);
		}
		finally {

		}
		return l.toArray(new AnalysisEvent[l.size()]);
		}
	@Override
	public AnalysisEvent[] retrieveEvents(AFEventSearchInfo sInfo, Integer maxEvents, boolean allowDups){
		ArrayList<AnalysisEvent> l = new ArrayList<AnalysisEvent>();

		Bson retrieveFilter = getBsonFilter(sInfo);
		if (retrieveFilter == null)
			return new AnalysisEvent[0];

		try { 
			FindIterable<Document> docs = null;
			if (sInfo.getAscending())
				docs = c_MongoCollection.find(retrieveFilter).sort(Sorts.ascending ("Timestamp") );
			else
				docs = c_MongoCollection.find(retrieveFilter).sort(Sorts.descending ("Timestamp") );
					
			int noEvents = (int) maxEvents;	
			long lastTime = 0L;
			int dupsFound = 0;
			String path = null;
			long leastDif = Long.MAX_VALUE;
			for (Document doc : docs) {
				long nextTime  = doc.getLong("Timestamp").longValue();
				path =doc.getString("Path");			
				if (!allowDups && (lastTime == nextTime)){
					dupsFound++;	
					continue;
				}
				if (noEvents > 0 ){			
					AnalysisData ad =  EventUtility.retrievePayload(get_PayloadType(), doc.getString("Data"));
					AnalysisEvent ev = new AnalysisEvent(nextTime, path, ad);
					setLocationForEvent(ev, doc);
					l.add(ev);	
					noEvents--;
					lastTime = nextTime;
				}
			} 
		}
		catch (Exception e){
			setError("Failed to retrieve events e>"+e);
		}
		finally {

		}
		return l.toArray(new AnalysisEvent[l.size()]);
	}
	@Override
	public String getPersistorType(){
		StringBuilder sb = new StringBuilder();
		sb.append("MongoDB: ");
		return sb.toString();
	}
	@Override
	public void storeEvent(AnalysisEvent event){
		try {
			insertOneEvent(event);
			AFEventDataInfo.adjustEventDataInfo(event);
		}
		catch (Exception e){
			setException("storeEvent(): Failed to write event record",e);
		}
	}
	@Override
	public void storeEvents(AnalysisEvent[] events){
		try {
			for(AnalysisEvent event: events)
				insertOneEvent(event);
			AFEventDataInfo.adjustEventDataInfo(events);
		}
		catch (Exception e){
			setException("storeEvents(): Failed to write event record",e);			
		}

	}
	// MongoDB SUPPORTING Methods -----------------------------------------------
	private void initConnection(){
		try {
			initMongoURI();
			initMongoClient();
		}
		catch(RuntimeException e){
			setError("Unable to connect to MongoDB RUNTIME EXCEPTION e>"+e);			
		}
		catch(Exception e){
			setError("Unable to connect to MongoDB EXCEPTION e>"+e);			
		}
	}
	private void insertOneEvent(AnalysisEvent ev){

		//	TIMESTAMP , SAVETILL, SOURCE_CONTAINER, SOURCE_IP, PATH, LABEL, DATA) VALUES (?
		Document doc = new Document();
		doc.put("Timestamp", ev.getTimestamp());
		doc.put("SaveTill", ev.getSaveTill());
		doc.put("SourceContainer", ev.getOriginatingContainer());
		doc.put("SourceIP", ev.getOriginatingIP());
		doc.put("Path",ev.getPath());
		doc.put("Label",ev.getLabel());
		doc.put("Latitude", ev.getLatitude());
		doc.put("Longitude", ev.getLongitude());	
		doc.put("Altitude", ev.getAltitude());	
		doc.put("Data",EventUtility.formatPayload(get_PayloadType(), ev.getAnalysisData()));
		c_MongoCollection.insertOne(doc);
	}
	private void initMongoURI() throws Exception {
		
		StringBuilder sb = new StringBuilder();
		sb.append("mongodb://");
		
		/*if (c_User != null && c_User.length() > 0){
			sb.append(c_User);
			sb.append(":");
			sb.append(c_Password);
			sb.append("@");
		}*/
		sb.append(c_Server);
		sb.append(":");
		sb.append(c_Port);
		c_MongoURIasString = sb.toString();
		c_MongoURI = new MongoClientURI(c_MongoURIasString);
	}
	private void initMongoClient() throws Exception {
		// Check if User exists, add username and password to the opening.
	
		
		c_MongoClient = new MongoClient(c_MongoURI);
		// Database (name) should be an attribute defaulting to VDAB
		c_MongoDatabase = c_MongoClient.getDatabase(c_DatabaseName);
		c_MongoCollection = c_MongoDatabase.getCollection(COLLECTION_NAME);
	}
	private static String getEventKey(String path, Document doc){
		StringBuilder sb = new StringBuilder(path);
		try {
			if (doc.getDouble("Latitude") != null)
				sb.append(doc.getDouble("Latitude"));
		} 
		catch (Exception e) {}
		try {
			if (doc.getDouble("Longitude")!= null)
				sb.append(doc.getDouble("Longitude"));
		} 
		catch (Exception e) {}
		return sb.toString();
	}
	private Bson getBsonFilter(AFEventSearchInfo sInfo){
		
		if (sInfo.getPath() != null)
			return (Filters.and(Filters.eq("Path",sInfo.getPath()),Filters.gt("Timestamp", sInfo.getOldest()),Filters.lte("Timestamp", sInfo.getYoungest())));
		if (sInfo.getLabel() != null)
			return (Filters.and(Filters.eq("Label",sInfo.getLabel()),Filters.gt("Timestamp", sInfo.getOldest()),Filters.lte("Timestamp", sInfo.getYoungest())));
		return null;
	}
	private void setLocationForEvent (AnalysisEvent ev, Document doc){

		try {
			Double lat  = doc.getDouble("Latitude");
			Double lng  = doc.getDouble("Longitude");
			Double alt  = doc.getDouble("Altitude");
			if (lat != null && lng != null ){
				ev.setLocation(Integer.valueOf(GeoUnits.DEGREES_NE_METERS),lat,lng, alt); 
			}

		}
		catch (Exception e) {
			setError("Unable to set event location e>"+e);
		}		
	}
}
