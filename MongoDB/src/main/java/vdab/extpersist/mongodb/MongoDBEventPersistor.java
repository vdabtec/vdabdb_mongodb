package vdab.extpersist.mongodb;


import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;
import org.bson.conversions.Bson;

import vdab.core.persistence.EventPersistor_A;
import vdab.core.persistence.EventUtility;

import com.lcrc.af.AnalysisData;
import com.lcrc.af.AnalysisDataDef;
import com.lcrc.af.AnalysisEvent;
import com.lcrc.af.AnalysisObject;
import com.lcrc.af.constants.TimeUnit;
import com.lcrc.af.datatypes.AFEventDataInfo;
import com.lcrc.af.datatypes.AFEventSearchInfo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;

public class MongoDBEventPersistor extends EventPersistor_A {
	private static AnalysisDataDef[] s_DBForEvent_ddefs = new AnalysisDataDef[]{ 
		AnalysisObject.getClassAttributeDataDef(MongoDBEventPersistor.class,"User")
		.setStandard().setEditOrder(10),
		AnalysisObject.getClassAttributeDataDef(MongoDBEventPersistor.class,"Password")
		.setAsPassword().setStandard().setEditOrder(11),
		AnalysisObject.getClassAttributeDataDef(MongoDBEventPersistor.class,"Server")
		.setRequired().setEditOrder(12),
		AnalysisObject.getClassAttributeDataDef(MongoDBEventPersistor.class,"Port")
		.setRequired().setEditOrder(13)
	};
	// https://www.mongodb.com/blog/post/getting-started-with-mongodb-and-java-part-i
	private static String COLLECTION_NAME = "EVENT";
	private static String DBNAME = "VDAB";

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
	public void purgeEvents(){
	//TODO - Implement
	// SQL ... DELETE FROM AF_EVENT WHERE SAVETILL < "+System.currentTimeMillis();

	}
	@Override
	public AFEventDataInfo[] getAllEventInfo(){
		ArrayList<AFEventDataInfo> l = new ArrayList<AFEventDataInfo>();
		// HACKALERT - Temporary need to get actual minimum and maximum time.
		//	EQUIVALENT SQL "SELECT SOURCE_CONTAINER, PATH, LABEL, MIN(TIMESTAMP),MAX(TIMESTAMP)FROM AF_EVENT GROUP BY SOURCE_CONTAINER, PATH, LABEL");

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
		}
		return l.toArray(new AFEventDataInfo[l.size()]);
	}
	@Override
	public AnalysisEvent[] retrieveEvents(AFEventSearchInfo sInfo, Integer maxEvents, boolean allowDups){
		ArrayList<AnalysisEvent> l = new ArrayList<AnalysisEvent>();

		Bson retrieveFilter = getBsonFilter(sInfo);

		try { 
			FindIterable<Document> docs = c_MongoCollection.find(retrieveFilter);
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
					l.add(new AnalysisEvent(nextTime, path, ad));	
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
		doc.put("Data",EventUtility.formatPayload(get_PayloadType(), ev.getAnalysisData()));
		c_MongoCollection.insertOne(doc);
	}
	private void initMongoURI() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("mongodb://");
		sb.append(c_Server);
		sb.append(":");
		sb.append(c_Port);
		c_MongoURIasString = sb.toString();
		c_MongoURI = new MongoClientURI(c_MongoURIasString);
	}
	private void initMongoClient() throws Exception {
		c_MongoClient = new MongoClient(c_MongoURI);
		c_MongoDatabase = c_MongoClient.getDatabase(DBNAME);
		c_MongoCollection = c_MongoDatabase.getCollection(COLLECTION_NAME);
	}
	private Bson getBsonFilter(AFEventSearchInfo sInfo){
		return (Filters.and(Filters.eq("Path",sInfo.getPath()),Filters.gt("Timestamp", sInfo.getOldest()),Filters.lte("Timestamp", sInfo.getYoungest())));
	}
}
