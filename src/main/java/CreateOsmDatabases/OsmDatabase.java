package CreateOsmDatabases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.OsmUser;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

public class OsmDatabase {
	
	static final String DB_URL_PREFIX = "jdbc:sqlite:";	
	
	Connection mConn=null;
	
	private String mSqlInsertNode="INSERT INTO nodes(id, lon, lat) VALUES(?,?,?)";
	private String mSqlInsertNodeTags="INSERT INTO node_tags(node_id, key, value) VALUES(?,?,?)";
	
	private String mSqlInsertWay="INSERT INTO ways(id) VALUES(?)";
	private String mSqlInsertWayTags="INSERT INTO way_tags(way_id, key, value) VALUES(?,?,?)";
	private String mSqlInsertWayNodes="INSERT INTO way_nodes (way_id, sequence, node_id) VALUES(?,?,?)";
		
	private String mSqlInsertRelation="INSERT INTO relations(id) VALUES(?)";
	private String mSqlInsertRelationTags="INSERT INTO relation_tags(rel_id, key, value) VALUES(?,?,?)";
	private String mSqlInsertRelationMembers="INSERT INTO relation_members (rel_id, sequence, member_type, member_id, member_role) VALUES(?,?,?,?,?)";
	
	private PreparedStatement mPrepStmtInsertNode=null;
	private PreparedStatement mPrepStmtInsertNodeTags=null;
	
	private PreparedStatement mPrepStmtInsertWay=null;
	private PreparedStatement mPrepStmtInsertWayTags=null;
	private PreparedStatement mPrepStmtInsertWayNodes=null;
	
	private PreparedStatement mPrepStmtInsertRelation=null;
	private PreparedStatement mPrepStmtInsertRelationTags=null;
	private PreparedStatement mPrepStmtInsertRelationMembers=null;
	
	private static final int UNKNOWN=-1;
	private static final int ASCENDING=0;
	private static final int DESCENDING=1;
	
	public OsmDatabase() {
		
	}
	
	public boolean openDatabase(String fileName) {
		
		// Open a connection
		Log.info("Opening SQLite database <"+fileName+">: ");
					
		try {
			mConn = DriverManager.getConnection(DB_URL_PREFIX+fileName);
			
		} catch (SQLException e) {
			
			Log.error("DriverManager getConnection error: "+e.getMessage());
			
			return false;
		}
		
		Log.info("Database <"+fileName+"> opened successfully");
		
		return true;
	}
	
	public boolean createDatabase(String fileName) {
		
		Statement stmt=null;
		String sql=null;
		
		try {
			
			// Open a connection
			System.out.println("Creating SQLite database <"+fileName+">: ");
			
			System.out.print(" - Establishing SQLite Connection... ");
			mConn = DriverManager.getConnection(DB_URL_PREFIX+fileName);
			System.out.println("Ok!");
			
			stmt = mConn.createStatement();
			
			sql = "DROP TABLE IF EXISTS nodes";
			System.out.print(" - Deleting table <nodes>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "DROP TABLE IF EXISTS node_tags";
			System.out.print(" - Deleting table <node_tags>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "DROP TABLE IF EXISTS ways";
			System.out.print(" - Deleting table <ways>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "DROP TABLE IF EXISTS way_tags";
			System.out.print(" - Deleting table <way_tags>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "DROP TABLE IF EXISTS way_nodes";
			System.out.print(" - Deleting table <way_nodes>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "DROP TABLE IF EXISTS relations";
			System.out.print(" - Deleting table <relations>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "DROP TABLE IF EXISTS relation_tags";
			System.out.print(" - Deleting table <relation_tags>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "DROP TABLE IF EXISTS relation_members";
			System.out.print(" - Deleting table <relation_members>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "CREATE TABLE nodes (\n"
					+ "	id INTEGER PRIMARY KEY,\n"
	                + "	lon REAL,\n"
	                + "	lat REAL\n"
	                + ");";
			System.out.print(" - Create table <nodes>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "CREATE TABLE node_tags (\n"
					+ "	node_id INTEGER,\n"
		    		+ " key TEXT,\n"
		    		+ " value TEXT\n"
	                + ");";
			System.out.print(" - Create table <node_tags>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "CREATE TABLE ways (\n"
					+ "	id INTEGER PRIMARY KEY\n"
	                + ");";
			System.out.print(" - Create table <ways>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "CREATE TABLE way_tags (\n"
					+ "	way_id INTEGER,\n"
		    		+ " key TEXT,\n"
		    		+ " value TEXT\n"
	                + ");";
			System.out.print(" - Create table <way_tags>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "CREATE TABLE way_nodes (\n"
					+ "	way_id INTEGER,\n"
					+ " sequence INTEGER,\n"
					+ " node_id INTEGER\n"
		    		+ ");";
			System.out.print(" - Create table <way_nodes>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "CREATE TABLE relations (\n"
					+ "	id INTEGER PRIMARY KEY\n"
	                + ");";
			System.out.print(" - Create table <relations>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
		    
		    sql = "CREATE TABLE relation_tags (\n"
					+ "	rel_id INTEGER,\n"
		    		+ " key TEXT,\n"
		    		+ " value TEXT\n"
	                + ");";
			System.out.print(" - Create table <relation_tags>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
			
		    sql = "CREATE TABLE relation_members (\n"
					+ "	rel_id INTEGER,\n"
					+ " sequence INTEGER,\n"
		    		+ " member_type INTEGER,\n"
		    		+ " member_id INTEGER,\n"
		    		+ " member_role TEXT\n"
	                + ");";
			System.out.print(" - Create table <relation_members>... ");
			stmt.executeUpdate(sql);
		    System.out.println("Ok!");
		    
		    sql = "CREATE INDEX node_tags_index ON node_tags(node_id)";
			System.out.print(" - Creating index of table <node_tags>... ");
			stmt.executeUpdate(sql);
			System.out.println("Ok!");
		    
			sql = "CREATE INDEX way_tags_index ON way_tags(way_id)";
			System.out.print(" - Creating index of table <way_tags>... ");
			stmt.executeUpdate(sql);
			System.out.println("Ok!");
		    
		    sql = "CREATE INDEX way_nodes_index ON way_nodes(way_id)";
			System.out.print(" - Creating index of table <way_nodes>... ");
			stmt.executeUpdate(sql);
			System.out.println("Ok!");
		    
		    sql = "CREATE INDEX relation_tags_index ON relation_tags(rel_id)";
			System.out.print(" - Creating index of table <relation_tags>... ");
			stmt.executeUpdate(sql);
			System.out.println("Ok!");
		    
		    sql = "CREATE INDEX relation_members_index ON relation_members(rel_id)";
			System.out.print(" - Creating index of table <relation_members>... ");
			stmt.executeUpdate(sql);
			System.out.println("Ok!");
		    
		    stmt.close();			
		}
		catch(SQLException se) {
			
			System.out.println("Error: "+se.getMessage());
			
			return false;
		}
		finally {
			
			//finally block used to close resources
		    try {
		    	
		    	if (stmt!=null)
		    		stmt.close();
		    }
		    catch(SQLException se2) {
		    	
		    }
		    
		}
		
		try {
			mPrepStmtInsertNode=mConn.prepareStatement(mSqlInsertNode);
			mPrepStmtInsertNodeTags=mConn.prepareStatement(mSqlInsertNodeTags);
			
			mPrepStmtInsertWay=mConn.prepareStatement(mSqlInsertWay);
			mPrepStmtInsertWayTags=mConn.prepareStatement(mSqlInsertWayTags);
			mPrepStmtInsertWayNodes=mConn.prepareStatement(mSqlInsertWayNodes);
			
			mPrepStmtInsertRelation=mConn.prepareStatement(mSqlInsertRelation);
			mPrepStmtInsertRelationTags=mConn.prepareStatement(mSqlInsertRelationTags);
			mPrepStmtInsertRelationMembers=mConn.prepareStatement(mSqlInsertRelationMembers);
			
			
		} catch (SQLException e) {
			
			Log.error("Error prepareStatement(): "+e.getMessage());
		}
		
		return true;
	}
	
	/*
	public void createIndexes() {
		
		Statement stmt=null;
		String sql=null;
		
		System.out.println("Creating indexes:");
		
		Instant start, end;
		
		try {
			
			start=Instant.now();
			
			mConn.setAutoCommit(false);
			
			
		    
		    mConn.commit();

		    mConn.setAutoCommit(true);
		    
		}
		catch (SQLException e) {
			System.out.println("Error: "+e.getMessage());
		}
		
	}
	*/
	
	public synchronized boolean addNode(Node node) {
		
		//String sql = "INSERT INTO nodes(id, lon, lat) VALUES(?,?,?)";
		
		long nodeId=node.getId();
		
		double lon=node.getLongitude();
		double lat=node.getLatitude();
		
		//PreparedStatement pstmt=null;
		 
        try {
        	
        	mPrepStmtInsertNode.setLong(1, nodeId);
        	mPrepStmtInsertNode.setDouble(2, lon);
        	mPrepStmtInsertNode.setDouble(3, lat);
        	mPrepStmtInsertNode.executeUpdate();
            
        	/*
        	pstmt = mConn.prepareStatement(sql);
        	
        	pstmt.setLong(1, nodeId);
            pstmt.setDouble(2, lon);
            pstmt.setDouble(3, lat);
            pstmt.executeUpdate();
            pstmt.close();
            */
        }
        catch (SQLException e) {
        	
            System.out.println("addNode(): ERROR: "+e.getMessage());
            
            return false;
        }
        /*
        finally {
        	
        	 try {
 		    	
 		    	if (pstmt!=null)
 		    		pstmt.close();
 		    }
 		    catch(SQLException se2) {
 		    	
 		    }
        }
        */
        
        Collection<Tag> tags=node.getTags();
        
        Iterator<Tag> tagIter=tags.iterator();
        
        while(tagIter.hasNext()) {
        	
        	Tag tag=tagIter.next();
        	
        	String key=tag.getKey();
        	String value=tag.getValue();
        	
        	//sql = "INSERT INTO node_tags(node_id, key, value) VALUES(?,?,?)";
    		
    		try {
            	
            	//pstmt = mConn.prepareStatement(sql);
    			
    			mPrepStmtInsertNodeTags.setLong(1, nodeId);
    			mPrepStmtInsertNodeTags.setString(2, key);
            	mPrepStmtInsertNodeTags.setString(3, value);
            	mPrepStmtInsertNodeTags.executeUpdate();
                
            	/*
    			pstmt.setLong(1, nodeId);
            	pstmt.setString(2, key);
            	pstmt.setString(3, value);
                pstmt.executeUpdate();
                pstmt.close();
                */
            }
            catch (SQLException e) {
            	
                System.out.println("addNode() tag: ERROR: "+e.getMessage());
                
                return false;
            }
    		/*
    		finally {
            	
           	 try {
    		    	
    		    	if (pstmt!=null)
    		    		pstmt.close();
    		    }
    		    catch(SQLException se2) {
    		    	
    		    }
           	}
           	*/
        }
		
		return true;
	}
	
	public synchronized boolean addWay(Way way) {
		
		//String sql = "INSERT INTO ways(id) VALUES(?)";
		
		long wayId=way.getId();
		
		//PreparedStatement pstmt=null;
		 
        try {
        	
        	//pstmt = mConn.prepareStatement(sql);
        	
        	mPrepStmtInsertWay.setLong(1, wayId);
        	mPrepStmtInsertWay.executeUpdate();
        	
            //pstmt.close();
        }
        catch (SQLException e) {
        	
            System.out.println("addWay(): ERROR: "+e.getMessage());
            
            return false;
        }
        /*
        finally {
        	
        	 try {
 		    	
 		    	if (pstmt!=null)
 		    		pstmt.close();
 		    }
 		    catch(SQLException se2) {
 		    	
 		    }
        }
        */
        
        Collection<Tag> tags=way.getTags();
        
        Iterator<Tag> tagIter=tags.iterator();
        
        while(tagIter.hasNext()) {
        	
        	Tag tag=tagIter.next();
        	
        	String key=tag.getKey();
        	String value=tag.getValue();
        	
        	//sql = "INSERT INTO way_tags(way_id, key, value) VALUES(?,?,?)";
    		
    		try {
            	
            	//pstmt = mConn.prepareStatement(sql);
            	
    			mPrepStmtInsertWayTags.setLong(1, wayId);
    			mPrepStmtInsertWayTags.setString(2, key);
            	mPrepStmtInsertWayTags.setString(3, value);
            	mPrepStmtInsertWayTags.executeUpdate();
                //pstmt.close();
            }
            catch (SQLException e) {
            	
                System.out.println("addWay() tag: ERROR: "+e.getMessage());
                
                return false;
            }
    		/*
    		finally {
            	
           	 try {
    		    	
    		    	if (pstmt!=null)
    		    		pstmt.close();
    		    }
    		    catch(SQLException se2) {
    		    	
    		    }
           	}
           	*/
        }
        
        Collection<WayNode> wayNodes=way.getWayNodes();
        
        Iterator<WayNode> nodesIter=wayNodes.iterator();
        
        int memberSequence=0;
        
        while(nodesIter.hasNext()) {
        	
        	WayNode wayNode=nodesIter.next();
        	
        	long nodeId=wayNode.getNodeId();
        	
        	//sql = "INSERT INTO way_nodes (way_id, sequence, node_id)"+
        	//	  " VALUES(?,?,?)";
    		
    		try {
            	
            	//pstmt = mConn.prepareStatement(sql);
            	
    			mPrepStmtInsertWayNodes.setLong(1, wayId);
    			mPrepStmtInsertWayNodes.setInt(2, memberSequence);
            	mPrepStmtInsertWayNodes.setLong(3, nodeId);
            	mPrepStmtInsertWayNodes.executeUpdate();
                //pstmt.close();
            }
            catch (SQLException e) {
            	
                System.out.println("addWay() wayNode: ERROR: "+e.getMessage());
                
                return false;
            }
    		/*
    		finally {
            	
              	 try {
       		    	
       		    	if (pstmt!=null)
       		    		pstmt.close();
       		    }
       		    catch(SQLException se2) {
       		    	
       		    }        	
       		}
       		*/
    		
    		memberSequence++;
        }
		
		return true;
	}

	public synchronized boolean addRelation(Relation relation) {
	
		//String sql = "INSERT INTO relations(id) VALUES(?)";
		
		long relId=relation.getId();
		
		//PreparedStatement pstmt=null;
		 
        try {
        	
        	//pstmt = mConn.prepareStatement(sql);
        	
        	mPrepStmtInsertRelation.setLong(1, relId);
        	mPrepStmtInsertRelation.executeUpdate();
            //pstmt.close();
        }
        catch (SQLException e) {
        	
            System.out.println("addRelation(): ERROR: "+e.getMessage());
            
            return false;
        }
        /*
        finally {
        	
        	 try {
 		    	
 		    	if (pstmt!=null)
 		    		pstmt.close();
 		    }
 		    catch(SQLException se2) {
 		    	
 		    }        	
        }
        */
        
        Collection<Tag> tags=relation.getTags();
        
        Iterator<Tag> tagIter=tags.iterator();
        
        while(tagIter.hasNext()) {
        	
        	Tag tag=tagIter.next();
        	
        	String key=tag.getKey();
        	String value=tag.getValue();
        	
        	//sql = "INSERT INTO relation_tags(rel_id, key, value) VALUES(?,?,?)";
    		
    		try {
            	
            	//pstmt = mConn.prepareStatement(sql);
            	
    			mPrepStmtInsertRelationTags.setLong(1, relId);
    			mPrepStmtInsertRelationTags.setString(2, key);
            	mPrepStmtInsertRelationTags.setString(3, value);
            	mPrepStmtInsertRelationTags.executeUpdate();
                //pstmt.close();
            }
            catch (SQLException e) {
            	
                System.out.println("addRelation() tag: ERROR: "+e.getMessage());
                
                return false;
            }
    		/*
    		finally {
            	
           	 try {
    		    	
    		    	if (pstmt!=null)
    		    		pstmt.close();
    		    }
    		    catch(SQLException se2) {
    		    	
    		    }        	
    		}
    		*/
        }
        
        Collection<RelationMember> members=relation.getMembers();
        
        Iterator<RelationMember> memberIter=members.iterator();
        
        int memberSequence=0;
        
        while(memberIter.hasNext()) {
        	
        	RelationMember member=memberIter.next();
        	
        	long memberId=member.getMemberId();
        	String memberRole=member.getMemberRole();
        	
        	int memberType;
        	
        	switch(member.getMemberType()) {
        	
        	case Bound:
        		memberType=0;
        		break;
        		
        	case Node:
        		memberType=1;
        		break;
        		
        	case Way:
        		memberType=2;
        		break;
        		
        	case Relation:
        		memberType=3;
        		break;
        		
        	default:
        		System.out.println("addRelation() member: Unknown relation member type");
        		memberType=-1;
                break;
        	}
        	
        	//sql = "INSERT INTO relation_members (rel_id, sequence, member_type, member_id, member_role)"+
        	//	  " VALUES(?,?,?,?,?)";
    		
    		try {
            	
            	//pstmt = mConn.prepareStatement(sql);
            	
    			mPrepStmtInsertRelationMembers.setLong(1, relId);
    			mPrepStmtInsertRelationMembers.setInt(2, memberSequence);
            	mPrepStmtInsertRelationMembers.setInt(3, memberType);
            	mPrepStmtInsertRelationMembers.setLong(4, memberId);
            	mPrepStmtInsertRelationMembers.setString(5, memberRole);
            	mPrepStmtInsertRelationMembers.executeUpdate();
                //pstmt.close();
            }
            catch (SQLException e) {
            	
                System.out.println("addRelation() member: ERROR: "+e.getMessage());
                
                return false;
            }
    		/*
    		finally {
            	
              	 try {
       		    	
       		    	if (pstmt!=null)
       		    		pstmt.close();
       		    }
       		    catch(SQLException se2) {
       		    	
       		    }        	
       		}
       		*/
    		
    		memberSequence++;
        }
        
        return true;
	}
	
	public void runTest() {
		
		System.out.println("Starting runTest()...");
		
		long id=100;
		int version=0;
		
		try {
			mConn.setAutoCommit(false);
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		
		Date timestamp=new Date();
		
		OsmUser user=new OsmUser(0, "user");
		
		CommonEntityData entityData=new CommonEntityData(id, version, timestamp, user, 0);
		
		Node node=new Node(entityData, 0.0, 0.0);
		
		int NUM_NODES=10;
		
		long startTime=System.currentTimeMillis();
		
		for(int i=0; i<NUM_NODES; i++) {
			
			addNode(node);
			
			id++;
			
			node.setId(id);
		}
		
		long elapsedTime=System.currentTimeMillis()-startTime;
		
		String text;
		
		text=String.format(Locale.ENGLISH, "Added %d nodes in %.1f seconds", NUM_NODES, (float)elapsedTime/1000);
        System.out.println(text);
        
        text=String.format(Locale.ENGLISH, "%.1f nodes per second", (float)NUM_NODES/elapsedTime*1000);
        System.out.println(text);
        
        //long commitStartTime=System.currentTimeMillis();
        
        Instant start=Instant.now();
        
        try {
        	
        	System.out.print("Database commit()... ");
			
        	mConn.commit();
			
		} catch (SQLException e) {
			
			System.out.println("Failed(): "+e.getMessage());
			
			//e.printStackTrace();
		}
        
        Instant end=Instant.now();
        
        text=String.format("Ok!! (Commit took "+Util.timeFormat(start, end)+")");
        
        System.out.println(text);		
        
        System.out.println("runTest() finished!!");
	}
	
	public boolean setAutoCommit(boolean mode) {
		
		System.out.print("OsmDatabase::setAutoCommit() to <"+mode+">... ");
		
		try {
			mConn.setAutoCommit(mode);
			
			System.out.println("Ok!!");
		
		} catch (SQLException e) {
			
			System.out.println("ERROR: "+e.getMessage());
			
			return false;
		}
		
		return true;
	}
	
	public boolean commit() {
		
		try {
			
			System.out.print("OsmDatabase:commit()... ");
			
			//long commitStartTime=System.currentTimeMillis();
			
			Instant start=Instant.now();
			
			mConn.commit();
			
			Instant end=Instant.now();
			
			String text=String.format("Ok!! (Commit took "+Util.timeFormat(start, end)+")");
	        
	        System.out.println(text);	        
			
		} catch (SQLException e) {
			
			System.out.println("Failed!! Error="+e.getMessage());
			return false;
		}
		
		return true;
	}
	
	public Node getNodeById(long nodeId) {
		
		Node node=null;
		
		int version=0;
		Date timeStamp=null;
		OsmUser user=null;
		long changesetId=0;
		
		Collection<Tag> tags=getNodeTags(nodeId);
		
		if (tags==null) {
			
			System.out.println("Error while getting tags of node #"+nodeId);
			
			return null;
		}
		
		CommonEntityData entityData=new CommonEntityData(nodeId, version,
				timeStamp, user, changesetId, tags);
		
		Coord coord=getNodeCoord(nodeId);
		
		if (coord!=null)
			node=new Node(entityData, coord.mLat, coord.mLon);
		
		return node;
	}
	
	public Coord getNodeCoord(long nodeId) {
		
		Coord coord=null;
		
		String sql=
				"SELECT\n" + 
				" id,\n" + 
				" lon,\n" + 
				" lat\n" + 
				"FROM\n" + 
				" nodes\n" + 
				"WHERE\n" +
				" id=?";
		
		PreparedStatement pstmt=null;
		
		try {
			pstmt = mConn.prepareStatement(sql);
			pstmt.setLong(1, nodeId);
			
			ResultSet rs=pstmt.executeQuery();
			
			if (!rs.next()) {
				
				Log.error("getNodeCoord(): ResultSet is empty of node <"+nodeId+">");
				
				coord=null;
			}
			else {
				
				double lon=rs.getDouble("lon");
				double lat=rs.getDouble("lat");
				
				coord=new Coord(lat, lon);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			System.out.println("Error: "+e.getMessage());
			
			coord=null;
		}
		
		return coord;		
	}
	
	public Collection<Tag> getNodeTags(long nodeId) {
		
		//Instant start=Instant.now();
		
		ArrayList<Tag> tags=new ArrayList<Tag>();
		
		String sql=
				"SELECT\n" + 
				" node_id,\n" + 
				" key,\n" + 
				" value\n" + 
				"FROM\n" + 
				" node_tags\n" + 
				"WHERE\n" +
				" node_id=?";
		
		PreparedStatement pstmt=null;
		
		try {
			pstmt = mConn.prepareStatement(sql);
			pstmt.setLong(1, nodeId);
			
			ResultSet rs=pstmt.executeQuery();
			
			while(rs.next()) {
				
				String key=rs.getString("key");
				String value=rs.getString("value");
				
				Tag tag=new Tag(key, value);
				
				tags.add(tag);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			System.out.println("Error: "+e.getMessage());
			
			tags=null;
		}
		
		//Instant end=Instant.now();
		
		//Duration time=Duration.between(start, end);
		
		//Log.debug("getNodeTags took "+time.toMillis()+" ms");
		
		return tags;
	}
	
	public Way getWayById(long wayId) {
		
		int version=0;
		Date timeStamp=null;
		OsmUser user=null;
		long changesetId=0;
		
		Collection<Tag> tags=getWayTags(wayId);
		
		if (tags==null) {
			
			Log.error("Error while getting tags of way #"+wayId);
			
			return null;
		}
		
		List<WayNode> wayNodes=getWayNodes(wayId);
		
		if (wayNodes==null) {
			
			Log.error("Error while getting members of way #"+wayId);
			
			return null;
		}
		
		CommonEntityData entityData=new CommonEntityData(wayId, version,
				timeStamp, user, changesetId, tags);
		
		Way way=new Way(entityData, wayNodes);
		
		return way;
	}
	
	public Collection<Tag> getWayTags(long wayId) {
		
		ArrayList<Tag> tags=new ArrayList<Tag>();
		
		String sql=
				"SELECT\n" + 
				" way_id,\n" + 
				" key,\n" + 
				" value\n" + 
				"FROM\n" + 
				" way_tags\n" + 
				"WHERE\n" +
				" way_id=?";
		
		PreparedStatement pstmt=null;
		
		try {
			pstmt = mConn.prepareStatement(sql);
			pstmt.setLong(1, wayId);
			
			ResultSet rs=pstmt.executeQuery();
			
			while(rs.next()) {
				
				String key=rs.getString("key");
				String value=rs.getString("value");
				
				Tag tag=new Tag(key, value);
				
				tags.add(tag);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			System.out.println("Error: "+e.getMessage());
			
			tags=null;
		}
		
		return tags;
	}
	
	public List<WayNode> getWayNodes(long wayId) {
		
		ArrayList<WayNode> wayNodes=new ArrayList<WayNode>();
		
		String sql=
				"SELECT\n" + 
				" way_id,\n" + 
				" sequence,\n" + 
				" node_id\n" + 
				"FROM\n" + 
				" way_nodes\n" + 
				"WHERE\n" +
				" way_id=?\n"+
				"ORDER BY\n"+
				" sequence ASC";
		
		PreparedStatement pstmt=null;
		
		try {
			pstmt = mConn.prepareStatement(sql);
			pstmt.setLong(1, wayId);
			
			ResultSet rs=pstmt.executeQuery();
			
			while(rs.next()) {
				
				Long nodeId=rs.getLong("node_id");
				
				WayNode wayNode=new WayNode(nodeId);
				
				wayNodes.add(wayNode);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			System.out.println("Error: "+e.getMessage());
			
			wayNodes=null;
		}
		
		return wayNodes;
	}
	
	public Relation getRelationById(long relId) {
		
		int version=0;
		Date timeStamp=null;
		OsmUser user=null;
		long changesetId=0;
		
		Collection<Tag> tags=getRelationTags(relId);
		
		if (tags==null) {
			
			Log.error("Error while getting tags of relation #"+relId);
			
			return null;
		}
		
		List<RelationMember> members=getRelationMembers(relId);
		
		if (members==null) {
			
			Log.error("Error while getting members of relation #"+relId);
			
			return null;
		}
		
		CommonEntityData entityData=new CommonEntityData(relId, version,
				timeStamp, user, changesetId, tags);
		
		Relation relation=new Relation(entityData, members);
		
		return relation;
	}
	
	public Collection<Tag> getRelationTags(long relId) {
		
		ArrayList<Tag> tags=new ArrayList<Tag>();
		
		String sql=
				"SELECT\n" + 
				" rel_id,\n" + 
				" key,\n" + 
				" value\n" + 
				"FROM\n" + 
				" relation_tags\n" + 
				"WHERE\n" +
				" rel_id=?";
		
		PreparedStatement pstmt=null;
		
		try {
			pstmt = mConn.prepareStatement(sql);
			pstmt.setLong(1, relId);
			
			ResultSet rs=pstmt.executeQuery();
			
			while(rs.next()) {
				
				String key=rs.getString("key");
				String value=rs.getString("value");
				
				Tag tag=new Tag(key, value);
				
				tags.add(tag);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			System.out.println("Error: "+e.getMessage());
			
			tags=null;
		}
		
		return tags;
	}
	
	public List<RelationMember> getRelationMembers(long relId) {
		
		ArrayList<RelationMember> members=new ArrayList<RelationMember>();
		
		String sql=
				"SELECT\n" + 
				" rel_id,\n" + 
				" sequence,\n" + 
				" member_type,\n" + 
				" member_id,\n" + 
				" member_role\n" + 
				"FROM\n" + 
				" relation_members\n" + 
				"WHERE\n" +
				" rel_id=?\n"+
				"ORDER BY\n"+
				" sequence ASC";
		
		PreparedStatement pstmt=null;
		
		try {
			pstmt = mConn.prepareStatement(sql);
			pstmt.setLong(1, relId);
			
			ResultSet rs=pstmt.executeQuery();
			
			while(rs.next()) {
				
				int memberTypeIndex=rs.getInt("member_type");
				
				EntityType memberType;
				
				switch(memberTypeIndex) {
				
				case 0:
					memberType=EntityType.Bound;
					break;
	        		
	        	case 1:
	        		memberType=EntityType.Node;
	        		break;
	        		
	        	case 2:
	        		memberType=EntityType.Way;
	        		break;
	        		
	        	case 3:
	        		memberType=EntityType.Relation;
	        		break;
	        		
	        	default:
	        		Log.error("getRelationMembers(): Unknown member type index <"+memberTypeIndex+
	        				"> of relation <"+relId+">");
	        		return null;					
				}
				
				Long memberId=rs.getLong("member_id");
				
				String memberRole=rs.getString("member_role");
				
				RelationMember member=new RelationMember(memberId, memberType, memberRole);
				
				members.add(member);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			System.out.println("Error: "+e.getMessage());
			
			members=null;
		}
		
		return members;
	}
	
	public List<Long> getRelationsIdsByTags(Collection<Tag> tags) {
		
		if (tags==null) {
			Log.warning("getRelationsIdsByTags. tags is null!!");
			return null;
		}
		
		if (tags.isEmpty()) {
			
			Log.warning("getRelationsIdsByTags. tags is empty!!");
			return null;
		}
		
		// Create first query
		String sql=null;
		
		Iterator<Tag> iter=tags.iterator();
		
		while(iter.hasNext()) {
			
			Tag tag=iter.next();
			
			if (sql==null) {
				
				// This is the first tag
				
				sql=
					"SELECT rel_id\n"+ 
					"FROM relation_tags\n"+ 
					"WHERE key='"+tag.getKey()+"' AND value='"+tag.getValue()+"'\n";
			}
			else {
				
				// This is not the first tag. Add query to subqueries...
				
				sql=
					"SELECT rel_id\n"+
					"FROM relation_tags\n"+
					"WHERE rel_id IN (\n"+
					sql+
					")\n"+ 
					"AND key='"+tag.getKey()+"' AND value='"+tag.getValue()+"'\n";
			}
			
		}
		
		ArrayList<Long> relIds=new ArrayList<Long>();
		
		Statement stmt=null;
		
		try {
			stmt = mConn.createStatement();
			ResultSet rs=stmt.executeQuery(sql);
			
			while(rs.next()) {
				
				long relId=rs.getLong("rel_id");
				
				relIds.add(relId);
			}
			
			rs.close();
			
			stmt.close();
			
		} catch (SQLException e) {
			
			Log.error(e.getMessage());
			
			relIds=null;
		}	
		
		return relIds;
	}
	
	
	public List<Long> getRelationsIdsByType(String typeName) {
		
		ArrayList<Long> relIds=new ArrayList<Long>();
		
		PreparedStatement pstmt=null;
		
		String sql=
				"SELECT\n" + 
				" relations.id,\n" + 
				" key,\n" + 
				" value\n" + 
				"FROM\n" + 
				" relations\n" + 
				" INNER JOIN relation_tags ON relations.id = relation_tags.id\n" + 
				"WHERE\n" + 
				" relation_tags.key =\"type\" AND relation_tags.value=?";
		
		try {
			pstmt = mConn.prepareStatement(sql);
			pstmt.setString(1, typeName);
			ResultSet rs=pstmt.executeQuery();
			
			while(rs.next()) {
				
				long relId=rs.getLong("id");
				
				relIds.add(relId);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			Log.error(e.getMessage());
			
			relIds=null;
		}
				
		return relIds;
	}
	
	public List<Long> filterRelations(List<Long> input, String key, String value) {
		
		Instant start=Instant.now();
		
		List<Long> output = new ArrayList<Long>();
		
		Iterator<Long> iter=input.iterator();
		
		while(iter.hasNext()) {
			
			Long relId=iter.next();
			
			String sql=
					"SELECT\n" + 
					" rel_id,\n" + 
					" key,\n" + 
					" value\n" + 
					"FROM\n" + 
					" relation_tags\n" + 
					"WHERE\n" + 
					" id=? AND key=? AND value=?";
			
			PreparedStatement pstmt=null;
			
			try {
				pstmt = mConn.prepareStatement(sql);
				pstmt.setLong(1, relId);
				pstmt.setString(2, key);
				pstmt.setString(3, value);
				
				ResultSet rs=pstmt.executeQuery();
				
				if (rs.next()) {
					
					output.add(relId);
				}
				
				rs.close();
				
				pstmt.close();
			
			} catch (SQLException e) {
				
				Log.error(e.getMessage());

			}
		}
		
		Instant end=Instant.now();
		
		long time=Duration.between(start, end).toMillis();
		
		Log.debug("Filter took "+time+" ms");
		
		return output;
	}
	
	public void checkPTv2(List<Long> relIds) {
		
		Iterator<Long> relIter=relIds.iterator();
		
		while(relIter.hasNext()) {
			
			Long relId=relIter.next();
			
			//Log.debug("Checking PTv2 of relation with id="+relId);
			
			Relation rel=getRelationById(relId);
			
			if (rel!=null)
				checkPTv2Relation(rel);			
		}		
	}
	
	public void checkPTv2Relation(Relation relation) {
		
		boolean processed=false;
		
		Collection<Tag> relTags=relation.getTags();
		
		Iterator<Tag> tagIter=relTags.iterator();
		
		while(tagIter.hasNext()) {
			
			Tag tag=tagIter.next();
			
			if (tag.getKey().compareTo("type")==0) {
				
				if (tag.getValue().compareTo("route")==0) {
					
					checkPTv2Route(relation);
					
					processed=true;
					
					break;
				}
				else if (tag.getValue().compareTo("route_master")==0) {
				
					checkPTv2RouteMaster(relation);
					
					processed=true;
					
					break;
				}
				else {
					
					Log.warning("PTv2: Relation #"+relation.getId()+" has an incorrect type <"+tag.getValue()+">");
					
					break;
				}
			}				
		}
		
		if (!processed) {
			
			Log.warning("PTv2: Relation #"+relation.getId()+" is not a route or route_master");
		}
	}
	
	public void checkPTv2RouteMaster(Relation relation) {
		
		boolean hasPTv2Tag=false;
		boolean isBusRoute=false;
		
		Collection<Tag> relTags=relation.getTags();
		
		Iterator<Tag> tagIter=relTags.iterator();
		
		while(tagIter.hasNext()) {
			
			Tag tag=tagIter.next();
			
			if (tag.getKey().compareTo("route_master")==0) {
				
				if (tag.getValue().compareTo("bus")==0) {
					
					isBusRoute=true;
				}
			}
			else if (tag.getKey().compareTo("public_transport:version")==0) {
				
				if (tag.getValue().compareTo("2")==0) {
					
					hasPTv2Tag=true;						
				}
			}
		}
		
		if (!hasPTv2Tag) {
			
			Log.warning("PTv2: Master Route Relation #"+relation.getId()+" has no <public_transport:version=2> tag");
		}
		
		if (!isBusRoute) {
			
			Log.warning("PTv2: Master Route Relation #"+relation.getId()+" is not a bus route");
		}
		
		int numberOfRoutes=0;
		
		List<RelationMember> members=relation.getMembers();
		
		for(int pos=0; pos<members.size(); pos++) {
			
			RelationMember member=members.get(pos);
			
			if (!member.getMemberRole().isEmpty()) {
				
				Log.warning("PTv2: Master Route Relation #"+relation.getId()+": Member in pos <"+pos+
						"> does not have an empty role <"+member.getMemberRole()+">");
			}
			
			if (member.getMemberType()==EntityType.Relation) {
				
				Relation routeRel=getRelationById(member.getMemberId());
				
				checkPTv2Route(routeRel);
				
				numberOfRoutes++;				
			}
			else {
				
				Log.warning("PTv2: Master Route Relation #"+relation.getId()+": Member in pos <"+pos+
						"> is not a relation");
			}				
		}
		
		if (numberOfRoutes<1) {
			
			Log.warning("PTv2: Master Route Relation #"+relation.getId()+": Master Route does not have any relation");			
		}
		else if (numberOfRoutes<2) {
			
			Log.warning("PTv2: Master Route Relation #"+relation.getId()+": Master Route only has 1 relation");			
		}
	}
	
	public void checkPTv2Route(Relation relation) {
		
		//Log.debug("PTv2: Checking relation <"+relation.getId()+">");
		
		// Step #1: Check for tag 'public_transport:version=2'
		
		boolean hasPTv2Tag=false;
		
		Collection<Tag> relTags=relation.getTags();
		
		Iterator<Tag> tagIter=relTags.iterator();
		
		while(tagIter.hasNext()) {
			
			Tag tag=tagIter.next();
			
			if (tag.getKey().compareTo("public_transport:version")==0) {
				
				if (tag.getValue().compareTo("2")==0) {
					
					hasPTv2Tag=true;						
				}
			}				
		}
		
		if (!hasPTv2Tag) {
			
			Log.warning("PTv2: Relation #"+relation.getId()+" has no <public_transport:version=2> tag");
		}
		
		// Step #2: Check list of stops and platforms
		
		List<Long> stopNodesIds=new ArrayList<Long>();
		List<Long> waysIds=new ArrayList<Long>();
		List<Long> linkNodesIds=new ArrayList<Long>();
				
		List<RelationMember> members=relation.getMembers();
		
		boolean foundEmptyRole=false;
		boolean detectedIncorrectStopPos=false;
		boolean detectedIncorrectPlatformPos=false;
		
		Coord stopNodeCoord=null;
		
		for(int pos=0; pos<members.size(); pos++) {
		
			RelationMember member=members.get(pos);
			
			if (member.getMemberRole().compareTo("stop")==0) {
				
				if (foundEmptyRole) {
					
					// Detected a stop member in an incorrect position (shall be located
					// at the beginning of the relation)
					
					detectedIncorrectStopPos=true;
				}
				
				if (member.getMemberType()==EntityType.Node) {
					
					if (pos==(members.size()-1)) {
						
						Log.warning("PTv2: Relation #"+relation.getId()+": Stop member node <"+member.getMemberId()+
								"> in pos <"+pos+"> is not followed by a <platform> member");
						
						stopNodeCoord=null;
					}
					else {
						
						String nextMemberRole=members.get(pos+1).getMemberRole();
						
						if (nextMemberRole.compareTo("platform")!=0) {
							
							Log.warning("PTv2: Relation #"+relation.getId()+": Stop member node <"+member.getMemberId()+
									"> in pos <"+pos+"> is not followed by a <platform> member");
						}
						
					}
					
					// Check that stop node has correct attributes
					
					boolean foundPublicTransportTag=false;
					
					Node stopNode=getNodeById(member.getMemberId());
					
					if (stopNode==null)
						continue;
					
					Collection<Tag> tags=stopNode.getTags();
					
					Iterator<Tag> nodeTagIter=tags.iterator();
					
					while(nodeTagIter.hasNext()) {
						
						Tag tag=nodeTagIter.next();
						
						if (tag.getKey().compareTo("public_transport")==0) {
							
							foundPublicTransportTag=true;
							
							if (tag.getValue().compareTo("stop_position")!=0) {
								
								Log.warning("PTv2: Relation #"+relation.getId()+": Stop node <"+member.getMemberId()+
										"> in pos <"+pos+"> has incorrect <public_transport> tag="+tag.getValue());								
							}
						}
					}
					
					if (!foundPublicTransportTag) {
						
						Log.warning("PTv2: Relation #"+relation.getId()+": Stop node <"+member.getMemberId()+
								"> in pos <"+pos+"> does not have the <public_transport> tag");
					}
					
					Long nodeId=member.getMemberId();
					
					stopNodesIds.add(nodeId);
					
					stopNodeCoord=new Coord(stopNode.getLatitude(), stopNode.getLongitude());
				}
				else {
					
					// Stop is not a node. Weird....
					
					Log.warning("PTv2: Relation #"+relation.getId()+": Stop member in pos <"+pos+"> is not a node...");
				}				
			}
			else if (member.getMemberRole().compareTo("platform")==0) {
				
				if (foundEmptyRole) {
					
					// Detected a platform member in an incorrect position (shall be located
					// at the beginning of the relation)
					
					detectedIncorrectPlatformPos=true;
				}
				
				if (member.getMemberType()==EntityType.Node) {
					
					Node platformNode=getNodeById(member.getMemberId());
					
					if (platformNode==null)
						continue;
					
					if (pos==0) {
						
						Log.warning("PTv2: Relation #"+relation.getId()+": Platform member node <"+
								member.getMemberId()+"> in pos <"+pos+"> does not follow a <stop> member");
						
						stopNodeCoord=null;
					}
					else {
						
						String previousMemberRole=members.get(pos-1).getMemberRole();
						
						if (previousMemberRole.compareTo("stop")!=0) {
							
							Log.warning("PTv2: Relation #"+relation.getId()+": Platform member node <"+
									member.getMemberId()+"> in pos <"+pos+"> does not follow a <stop> member");
						}
						else {
							
							// Check that distance between stop and platform nodes is less than 20 meters
							
							Coord platCoord=new Coord(platformNode.getLatitude(), platformNode.getLongitude());
							
							double distance=stopNodeCoord.distanceTo(platCoord);
							
							if (distance>20.0) {
								
								Log.warning("PTv2: Relation #"+relation.getId()+": Distance between platform member node <"+
										member.getMemberId()+"> and stop position is too big <"+
										String.format("%.1f", distance)+" meters>");
								
							}
							
						}
					}
					
					// Check that platform node has correct attributes
					
					boolean foundPublicTransportTag=false;
					
					Collection<Tag> tags=platformNode.getTags();
					
					Iterator<Tag> nodeTagIter=tags.iterator();
					
					while(nodeTagIter.hasNext()) {
						
						Tag tag=nodeTagIter.next();
						
						if (tag.getKey().compareTo("public_transport")==0) {
							
							foundPublicTransportTag=true;
							
							if (tag.getValue().compareTo("platform")!=0) {
								
								Log.warning("PTv2: Relation #"+relation.getId()+": Platform node <"+member.getMemberId()+
										"> in pos <"+pos+"> has incorrect <public_transport> tag="+tag.getValue());								
							}
						}
					}
					
					if (!foundPublicTransportTag) {
						
						Log.warning("PTv2: Relation #"+relation.getId()+": Platform node <"+member.getMemberId()+
								"> in pos <"+pos+"> does not have the <public_transport> tag");
					}
				}
				else {
					
					// Platform is not a node. Weird....
					
					Log.warning("PTv2: Relation #"+relation.getId()+": Platform member in pos <"+pos+
							"> is not a node");
				}				
			}
			else if (member.getMemberRole().isEmpty()) {
				
				// Found an empty role member. It shall be a way
				
				if (member.getMemberType()!=EntityType.Way) {
					
					Log.warning("PTv2: Empty role relation member in pos <"+pos+"> of relation <"+relation.getId()+"> is not a way");
						
					continue;
				}
				else {
					
					waysIds.add(member.getMemberId());
					
					foundEmptyRole=true;					
				}
			}
			else {
				
				Log.warning("PTv2: Relation #"+relation.getId()+": Relation member in pos <"+pos+"> has incorrect role <"+
						member.getMemberRole()+">");
			}
		}
		
		if (detectedIncorrectStopPos || detectedIncorrectPlatformPos) {
			
			Log.warning("PTv2: Relation #"+relation.getId()+": Detected incorrect stop or platform position"+
					" (Shall be located at the beginning)");
		}
		
		// Step #3: Check continuity between route ways
		
		boolean orderedRoute=true;
		
		if (waysIds.size()==0) {
			
			// No ways detected. Nothing else to check..
			
			Log.warning("PTv2: Relation <"+relation.getId()+"> has no ways");
			
			orderedRoute=false;
		}
		else if (waysIds.size()==1) {
			
			// Route has only one way. There's no need to check continuity
			
			orderedRoute=false;			
		}
		else {
			
			long prevLinkNode1=-1;
			long prevLinkNode2=-1;
			long prevLinkNode=-1;
			
			for(int pos=0; pos<waysIds.size(); pos++) {
			
				Way way=getWayById(waysIds.get(pos));
			
				MyWay myWay=new MyWay(way);
			
				List<WayNode> wayNodes=way.getWayNodes();
				
				if (wayNodes.size()<2) {
					
					Log.warning("PTv2: Way <"+way.getId()+"> has less than 2 nodes");
					
					orderedRoute=false;
					
					continue;
				}
			
				//long nodeId1=wayNodes.get(0).getNodeId();
				//long nodeId2=wayNodes.get(wayNodes.size()-1).getNodeId();
				
				long nodeId1=myWay.getFirstNodeId();
				long nodeId2=myWay.getLastNodeId();
				
				long nextLinkNode=-1;
				
				//Log.debug("PTv2: Way #"+pos+", Node1="+nodeId1+", Node2="+nodeId2);
					
				if (pos==0) {
					
					// This is the first way
					
					prevLinkNode1=nodeId1;
					prevLinkNode2=nodeId2;
					prevLinkNode=-1;
				}
				else {
					
					if (prevLinkNode<0) {
					
						// We come the first way or from a non-continuity situation
						
						if ((nodeId1==prevLinkNode1) || (nodeId1==prevLinkNode2)) {
						
							prevLinkNode=nodeId1;
							
							nextLinkNode=nodeId2;
						}
						else if ((nodeId2==prevLinkNode1) || (nodeId2==prevLinkNode2)) {
						
							prevLinkNode=nodeId2;
							
							nextLinkNode=nodeId1;
						}
						else {
						
							Log.warning("PTv2: Relation #"+relation.getId()+": Detected non continuity in way <"+
									way.getId()+">");
							
							orderedRoute=false;
							
							prevLinkNode1=nodeId1;
							prevLinkNode2=nodeId2;
							prevLinkNode=-1;
							nextLinkNode=-1;
						}
					}
					else {
						
						// We come from a good continuity situation
						
						if (nodeId1==prevLinkNode) {
							
							prevLinkNode=nodeId1;
							
							nextLinkNode=nodeId2;
						}
						else if (nodeId2==prevLinkNode) {
							
							prevLinkNode=nodeId2;
							
							nextLinkNode=nodeId1;
						}
						else {
							
							Log.warning("PTv2: Relation #"+relation.getId()+": Detected non continuity in node <"+
									prevLinkNode+">");
							
							//Log.debug("PTv2: Pos="+pos+", wayId="+way.getId());							
							//Log.debug("PTv2: PrevLinkNode="+prevLinkNode+", nodeId1="+nodeId1+", nodeId2="+nodeId2);							
							
							orderedRoute=false;
							
							prevLinkNode1=nodeId1;
							prevLinkNode2=nodeId2;
							prevLinkNode=-1;
							nextLinkNode=-1;
						}
					}
					
					linkNodesIds.add(prevLinkNode);
					
					prevLinkNode=nextLinkNode;
				}
			}
		}
		
		// Step #4: Check stop nodes
		
		if (stopNodesIds.size()==0) {
			
			Log.warning("PTv2: Relation #"+relation.getId()+" does not have any stop node");
			
			return;
		}
		
		if (stopNodesIds.size()<2) {
			
			Log.warning("PTv2: Relation #"+relation.getId()+" has only 1 stop node. It must have at least 2 (start and stop)");
		}
		
		if ((waysIds.size()!=0) && (waysIds.size()!=(linkNodesIds.size()+1))) {
			
			Log.warning("PTv2: Relation #"+relation.getId()+": num ways="+
					waysIds.size()+" is not num links="+linkNodesIds.size()+"+1");
		
		}
		
		for(int i=0; i<waysIds.size(); i++) {
			
			Way way=getWayById(waysIds.get(i));
			
			MyWay myWay=new MyWay(way);
			
			long nodeId1=myWay.getFirstNodeId();
			long nodeId2=myWay.getLastNodeId();
			
			//long prevLinkId;
			long nextLinkId;
			
			int wayDir=UNKNOWN;			
			
			if (i==0) {
				
				// This is the first way
				
				if (linkNodesIds.size()==0) {
					
					// There's only one way in the relation 
					
					nextLinkId=-1;
				}
				else {
					
					nextLinkId=linkNodesIds.get(i);
				}
				
				long startNodeId=stopNodesIds.get(0);
				
				//Log.debug("PTv2: startNodeId: "+startNodeId);
				//Log.debug("PTv2: first way Id: "+startNodeId);
				
				boolean startNodeFound;
				
				if (nextLinkId<0) {
					
					// There's no link id. The first stop node can be nodeId1 o nodeId2
					
					if (startNodeId==nodeId1) {
						
						// Start node detected in nodeId1...
						startNodeFound=true;

						wayDir=ASCENDING;
					}
					else if (startNodeId==nodeId2) {
						
						// Start node detected in nodeId2...
						startNodeFound=true;
						
						wayDir=DESCENDING;
					}
					else {
						
						startNodeFound=false;
						
						wayDir=UNKNOWN;
					}
				}
				else {
					
					if (nextLinkId==nodeId1) {
						
						// Start node shall be in node2
						
						if (startNodeId==nodeId2) {
							
							startNodeFound=true;
							
							wayDir=DESCENDING;
						}
						else {
							
							startNodeFound=false;
							
							wayDir=ASCENDING;
						}						
					}
					else if (nextLinkId==nodeId2) {
						
						// Start node shall be in node1
						
						if (startNodeId==nodeId1) {
							
							startNodeFound=true;
							
							wayDir=ASCENDING;
						}
						else {
							
							startNodeFound=false;
							
							wayDir=DESCENDING;
						}						
					}
					else {
						
						startNodeFound=false;
						
						wayDir=UNKNOWN;
					}
				}
				
				if (!startNodeFound) {
					
					Log.warning("PTv2: Relation #"+relation.getId()+": First stop node not detected in first way");
				}
				
								
				//Log.debug("PTv2 way order is "+(ascending? "ascending":"descending"));
			}
			else if (i==(waysIds.size()-1)) {
				
				// This is the last way. Check for final stop position
				
				long lastLinkId=linkNodesIds.get(linkNodesIds.size()-1);
				
				long endNodeId=stopNodesIds.get(stopNodesIds.size()-1);
				
				//Log.debug("PTv2: startNodeId: "+startNodeId);
				//Log.debug("PTv2: first way Id: "+startNodeId);
				
				boolean endNodeFound;
				
				if (lastLinkId<0) {
					
					// There's no link id. The first stop node can be nodeId1 o nodeId2
					
					if (endNodeId==nodeId1) {
						
						// End node detected in nodeId1...
						endNodeFound=true;

						wayDir=DESCENDING;
					}
					else if (endNodeId==nodeId2) {
						
						// End node detected in nodeId2...
						endNodeFound=true;
						
						wayDir=ASCENDING;						
					}
					else {
						
						endNodeFound=false;
						
						wayDir=UNKNOWN;
					}
				}
				else {
					
					if (lastLinkId==nodeId1) {
						
						// End node shall be in node2
						
						if (endNodeId==nodeId2) {
							
							endNodeFound=true;
							
							wayDir=ASCENDING;
						}
						else {
							
							endNodeFound=false;
							
							wayDir=ASCENDING;
						}						
					}
					else if (lastLinkId==nodeId2) {
						
						// End node shall be in node1
						
						if (endNodeId==nodeId1) {
							
							endNodeFound=true;
							
							wayDir=DESCENDING;
						}
						else {
							
							endNodeFound=false;
							
							wayDir=DESCENDING;
						}						
					}
					else {
						
						endNodeFound=false;
						
						wayDir=UNKNOWN;
					}
				}
				
				if (!endNodeFound) {
					
					Log.warning("PTv2: Relation #"+relation.getId()+": End stop node not detected in last way");
				}
			}
			else {
				
				// This is not the first nor the last way
				
				long prevLink=linkNodesIds.get(i-1);
				long nextLink=linkNodesIds.get(i);
				
				if (prevLink==nodeId1) {
					
					if (nextLink==nodeId2) {
						
						wayDir=ASCENDING;						
					}
					else {
						
						wayDir=UNKNOWN;
					}
				}
				else if (prevLink==nodeId2) {
					
					if (nextLink==nodeId1) {
						
						wayDir=DESCENDING;						
					}
					else {
						
						wayDir=UNKNOWN;
					}
				}
				else {
					
					wayDir=UNKNOWN;
				}				
			}
			
			// Check if it's a valid way for public transport
			
			String type=myWay.getHighwayType();
			
			if (type==null) {
				
				Log.warning("PTv2: Relation #"+relation.getId()+": Way <"+way.getId()+"> is not a highway");
				
				break;
			}
			else if (type.compareTo("motorway")==0 ||
				type.compareTo("motorway_link")==0 ||
				type.compareTo("trunk")==0 ||
				type.compareTo("trunk_link")==0 ||
				type.compareTo("primary")==0 ||
				type.compareTo("primary_link")==0 ||
				type.compareTo("secondary")==0 ||
				type.compareTo("secondary_link")==0 ||
				type.compareTo("tertiary")==0 ||
				type.compareTo("tertiary_link")==0 ||
				type.compareTo("unclassified")==0 ||
				type.compareTo("residential")==0 ||
				type.compareTo("service")==0 ||
				type.compareTo("track")==0) {
				
				// This is a correct highway type
			}
			else {
				
				Log.warning("PTv2: Relation #"+relation.getId()+": incorrect <highway> tag <"+type+"> of way <"+
						way.getId()+"> is not correct");
			}
			
			boolean isLink=type.endsWith("_link");
			
			boolean isRoundabout=myWay.isRoundabout();
			
			// Check way direction
			
			if (orderedRoute) {
				
				if (wayDir==UNKNOWN) {
					
					Log.warning("PTv2: Relation #"+relation.getId()+": Direction of way <"+way.getId()+"> is unknown");
				}
				else if (wayDir==ASCENDING || wayDir==DESCENDING) {
					
					int oneway=myWay.getOneway();
					
					if (oneway==myWay.NO_ONEWAY) {
						
						if (isLink || isRoundabout) {
							
							oneway=myWay.ONEWAY_FORWARD;
						}
					}
					
					if (wayDir==ASCENDING && oneway==myWay.ONEWAY_BACKWARD) {
						
						Log.warning("PTv2: Relation #"+relation.getId()+": Direction of way <"+way.getId()+"> is backward");						
					}
					else if (wayDir==DESCENDING && oneway==myWay.ONEWAY_FORWARD) {
						
						Log.warning("PTv2: Relation #"+relation.getId()+": Direction of way <"+way.getId()+"> is forward");						
					}					
					
				}
				else {
					
					Log.warning("PTv2: Relation #"+relation.getId()+": Direction of way <"+way.getId()+"> is not correct");
				}
			}
		}
		
		// Step #5: Check order of stop nodes
		
		if (!orderedRoute) {
			
			// This is not an ordered route. Skip check
			
			return;
		}
		
		/*
		if (relation.getId()==9256704) {
			
			orderedRoute=true;
		}
		*/
		
		// First, get list of all the nodes of the route
		
		List<Long> routeNodes=new ArrayList<Long>();
		
		for(int i=0; i<waysIds.size(); i++) {
			
			Way way=getWayById(waysIds.get(i));
			
			MyWay myWay=new MyWay(way);
			
			long nodeId1=myWay.getFirstNodeId();
			long nodeId2=myWay.getLastNodeId();
			
			int wayDir=UNKNOWN;
			
			boolean isLastWay;
			
			if (i==(waysIds.size()-1)) {
				
				// This is the last way
				
				isLastWay=true;
				
				if (nodeId1==linkNodesIds.get(i-1)) {
					
					wayDir=ASCENDING;
				}
				else if (nodeId2==linkNodesIds.get(i-1)) {
					
					wayDir=DESCENDING;
				}

			}
			else {
				
				// This is NOT the last way
				
				isLastWay=false;
				
				if (nodeId1==linkNodesIds.get(i)) {
					
					wayDir=DESCENDING;
				}
				else if (nodeId2==linkNodesIds.get(i)) {
					
					wayDir=ASCENDING;
				}
		
			}
			
			if (wayDir==UNKNOWN) {
				
				Log.warning("PTv2: Relation #"+relation.getId()+": Order stop nodes. Link node not found in way <"+way.getId()+">");
				
				continue;				
			}
			
			List<WayNode> wayNodes=way.getWayNodes();
			
			//int numNodesToAdd;
			
			int offset;
			
			if (isLastWay) {
				
				// For the last way, add all the nodes
				
				//numNodesToAdd=wayNodes.size();
				offset=0;
			}
			else {
				
				// For the rest of ways, add all the nodes but the last one
				
				//numNodesToAdd=wayNodes.size()-1;
				
				offset=1;
			}
			
			if (wayDir==ASCENDING) {
				
				for(int j=0; j<(wayNodes.size()-offset); j++) {
					
					routeNodes.add(wayNodes.get(j).getNodeId());					
				}
			}
			else if (wayDir==DESCENDING) {
				
				for(int j=(wayNodes.size()-1); j>=offset; j--) {
					
					routeNodes.add(wayNodes.get(j).getNodeId());					
				}
				
			}
			else {
				
				Log.warning("PTv2: Relation #"+relation.getId()+": Order of way <"+way.getId()+"> is unknown");
				
			}
		}
		
		//Log.info("PTv2: Relation #"+relation.getId()+": Number of route nodes: "+routeNodes.size());
		
		int startPos=0;
		
		for(int i=0; i<stopNodesIds.size(); i++) {
			
			long stopNodeId=stopNodesIds.get(i);
			
			int pos=startPos;
			
			boolean stopNodeFound=false;
			
			while (pos<routeNodes.size()) {
				
				if (stopNodeId==routeNodes.get(pos)) {
					
					stopNodeFound=true;
					
					break;
				}
				
				pos++;
			}
			
			if (stopNodeFound) {
				
				startPos=pos;
			}
			else {
				
				Log.warning("PTv2: Relation #"+relation.getId()+": Stop node <"+stopNodeId+"> is not ordered");
				
				pos=0;
				
				while (pos<startPos) {
					
					if (stopNodeId==routeNodes.get(pos)) {
						
						stopNodeFound=true;
						
						break;
					}
					
					pos++;
				}
				
				if (stopNodeFound) {
					
					startPos=pos;
				}
				else {
					
					Log.warning("PTv2: Relation #"+relation.getId()+": Stop node <"+stopNodeId+"> not found in route");					
				}
		
			}
			
		}
		
		/*
		List<Integer> stopNodesOrder=new ArrayList<Integer>();
		
		// Reset stop nodes order index
		
		while(stopNodesOrder.size()<stopNodesIds.size()) {
			
			stopNodesOrder.add(-1);
		}
		
		int currentNodePos=0;
		*/
		
		
	}
	
	public void checkHiking(List<Long> relIds) {
		
		Iterator<Long> relIter=relIds.iterator();
		
		while(relIter.hasNext()) {
			
			Long relId=relIter.next();
			
			Relation rel=getRelationById(relId);
			
			if (rel!=null)
				checkHikingRelation(rel);			
		}		
	}
	
	public void checkHikingRelation(Relation relation) {
		
		boolean processed=false;
		
		Collection<Tag> relTags=relation.getTags();
		
		Iterator<Tag> tagIter=relTags.iterator();
		
		while(tagIter.hasNext()) {
			
			Tag tag=tagIter.next();
			
			if (tag.getKey().compareTo("type")==0) {
				
				if (tag.getValue().compareTo("route")==0) {
					
					checkHikingRoute(relation);
					
					processed=true;
					
					break;
				}
				else if (tag.getValue().compareTo("superroute")==0) {
				
					checkHikingSuperRoute(relation);
					
					processed=true;
					
					break;
				}
				else {
					
					Log.warning("Hiking: Relation #"+relation.getId()+" has an incorrect type <"+tag.getValue()+">");
					
					break;
				}
			}				
		}
		
		if (!processed) {
			
			Log.warning("Hiking: Relation #"+relation.getId()+" is not a route or superroute");
		}
	}
	
public void checkHikingSuperRoute(Relation relation) {
		
		//boolean hasPTv2Tag=false;
		boolean isHikingRoute=false;
		
		Collection<Tag> relTags=relation.getTags();
		
		Iterator<Tag> tagIter=relTags.iterator();
		
		while(tagIter.hasNext()) {
			
			Tag tag=tagIter.next();
			
			if (tag.getKey().compareTo("route")==0) {
				
				if (tag.getValue().compareTo("hiking")==0) {
					
					isHikingRoute=true;
				}
			}
			/*
			else if (tag.getKey().compareTo("public_transport:version")==0) {
				
				if (tag.getValue().compareTo("2")==0) {
					
					hasPTv2Tag=true;						
				}
			}
			*/
		}
		
		/*
		if (!hasPTv2Tag) {
			
			Log.warning("PTv2: Master Route Relation #"+relation.getId()+" has no <public_transport:version=2> tag");
		}
		*/
		
		if (!isHikingRoute) {
			
			Log.warning("Hiking: Super Route Relation #"+relation.getId()+" is not a hiking route");
		}
		
		int numberOfRoutes=0;
		
		List<RelationMember> members=relation.getMembers();
		
		for(int pos=0; pos<members.size(); pos++) {
			
			RelationMember member=members.get(pos);
			
			if (!member.getMemberRole().isEmpty()) {
				
				Log.warning("Hiking: Super Route Relation #"+relation.getId()+": Member in pos <"+pos+
						"> does not have an empty role <"+member.getMemberRole()+">");
			}
			
			if (member.getMemberType()==EntityType.Relation) {
				
				Relation routeRel=getRelationById(member.getMemberId());
				
				checkHikingRoute(routeRel);
				
				numberOfRoutes++;				
			}
			else {
				
				Log.warning("Hiking: Super Route Relation #"+relation.getId()+": Member in pos <"+pos+
						"> is not a relation");
			}				
		}
		
		if (numberOfRoutes<1) {
			
			Log.warning("Hiking: Super Route Relation #"+relation.getId()+": Super Route does not have any relation");			
		}
		else if (numberOfRoutes<2) {
			
			Log.warning("Hiking: Super Route Relation #"+relation.getId()+": Super Route only has 1 relation");			
		}
	}
	
	public void checkHikingRoute(Relation relation) {
	
	}
}
