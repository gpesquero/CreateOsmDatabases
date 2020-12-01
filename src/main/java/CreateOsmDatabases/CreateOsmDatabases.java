package CreateOsmDatabases;

import java.io.File;
import java.io.FilenameFilter;

public class CreateOsmDatabases {
	
	// JDBC driver name and database URL
	//static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	//static final String DB_URL = "jdbc:mysql://localhost:3306/";
	
	//static final String DB_FILE = "C:/temp/prueba.db";
	
	//static final String PBFS_DIR_NAME = "PBFs";
	//static final String DBS_DIR_NAME = "DBs";
	
	static final String PBF_FILE_EXT =".osm.pbf";
	
	public static void main(String[] args) {
		
		System.out.println("Starting...");
		
		if (args.length<1) {
			
			System.out.println("Missing argument <BaseDir>. Quitting...");
			
			return;
		}
		
		String mapsDirName=args[0];
		
		System.out.println("Maps DirName=<"+mapsDirName+">");
		
		File mapsDir=new File(mapsDirName);
		
		if (!mapsDir.isDirectory()) {
			
			System.out.println("MapsDir <"+mapsDirName+"> is not a directory. Quitting...");
			
			return;
		}
		
		/*
		File pbfDir=new File(baseDir, PBFS_DIR_NAME);
		
		if (!pbfDir.isDirectory()) {
			
			System.out.println("PBF Dir <"+pbfDir.getAbsolutePath()+"> is not a directory. Quitting...");
			
			return;
		}
		*/
		
		/*
		File dbDir=new File(baseDir, DBS_DIR_NAME);
		
		if (!dbDir.isDirectory()) {
			
			if (!dbDir.mkdir()) {
				
				System.out.println("DB Dir <"+dbDir.getAbsolutePath()+"> cannot be created. Quitting...");
				
				return;					
			}
		}
		*/
		
		FilenameFilter filter=new FilenameFilter() {
			
			public boolean accept(File directory, String fileName) {
			        return fileName.endsWith(PBF_FILE_EXT);
			    }
			};
		
		File[] pbfFiles=mapsDir.listFiles(filter);
		
		System.out.println("Found "+pbfFiles.length+" PBF files (*"+PBF_FILE_EXT+")");
		
		for(int file=0; file<pbfFiles.length; file++) {
			
			OsmPbfFile pbf=new OsmPbfFile();
		
			if (!pbf.openFile(pbfFiles[file].getAbsolutePath())) {
			
				System.out.println("Error opening PBF File <"+pbfFiles[file].getAbsolutePath()+">. Go to next one...");
			
				continue;
			}
			
			String mapName=pbfFiles[file].getName().replaceFirst(PBF_FILE_EXT, "");
			
			System.out.println("Map Name: <"+mapName+">");
			
			File dbFile=new File(mapsDir, mapName+".db");
			
			System.out.println("DB Filename: <"+dbFile.getAbsolutePath()+">");
			
			if (dbFile.exists()) {
				
				System.out.println("DB <"+dbFile.getAbsolutePath()+"> already exists. Go to next one...");
				
				continue;
			}
			
			OsmDatabase db=new OsmDatabase();
			
			if (!db.createDatabase(dbFile.getAbsolutePath())) {
				
				System.out.println("Error opening OSM Database. Quitting...");
				
				return;
			}
			
			//db.createIndexes();	
			
			pbf.getObjectCount();
			
			//db.runTest();
			
			if (!pbf.process(db)) {
				
				System.out.println("Error processing PBF File <"+pbfFiles[file].getAbsolutePath()+">. Quitting...");
				
				return;
			}
			
			//db.createIndexes();			
		}
		
		
		
		System.out.println("Finished...");	
	}
}
