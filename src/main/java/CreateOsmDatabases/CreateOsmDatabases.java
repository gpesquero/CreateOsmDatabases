package CreateOsmDatabases;

import java.io.File;
import java.io.FilenameFilter;

import org.utilslibrary.Log;
import org.utilslibrary.OsmDatabase;
import org.utilslibrary.OsmPbfFile;

public class CreateOsmDatabases {
	
	static final String APP_NAME = "CreateOsmDabases";
	
	static final String PBF_FILE_EXT =".osm.pbf";
	
	public static void main(String[] args) {
		
		Log.info("Starting " + APP_NAME + "...");
		
		if (args.length < 1) {
			
			Log.error("Missing argument <BaseDir>. Quitting...");
			
			return;
		}
		
		String mapsDirName = args[0];
		
		Log.info("Maps DirName=<" + mapsDirName + ">");
		
		File mapsDir = new File(mapsDirName);
		
		if (!mapsDir.isDirectory()) {
			
			Log.error("MapsDir <" + mapsDirName + "> is not a directory. Quitting...");
			
			return;
		}
		
		FilenameFilter filter = new FilenameFilter() {
			
			public boolean accept(File directory, String fileName) {
			        return fileName.endsWith(PBF_FILE_EXT);
			    }
			};
		
		File[] pbfFiles = mapsDir.listFiles(filter);
		
		Log.info("Found " + pbfFiles.length + " PBF files (*" + PBF_FILE_EXT + ")");
		
		for(int file = 0; file < pbfFiles.length; file++) {
			
			OsmPbfFile pbf = new OsmPbfFile();
		
			if (!pbf.openFile(pbfFiles[file].getAbsolutePath())) {
			
				Log.error("Error opening PBF File <" + pbfFiles[file].getAbsolutePath() + ">. Go to next one...");
			
				continue;
			}
			
			String mapName = pbfFiles[file].getName().replaceFirst(PBF_FILE_EXT, "");
			
			Log.info("Map Name: <" + mapName + ">");
			
			File dbFile = new File(mapsDir, mapName + ".db");
			
			Log.info("DB Filename: <" + dbFile.getAbsolutePath() + ">");
			
			if (dbFile.exists()) {
				
				Log.info("DB <" + dbFile.getAbsolutePath() + "> already exists. Go to next one...");
				
				continue;
			}
			
			OsmDatabase db = new OsmDatabase();
			
			if (!db.createDatabase(dbFile.getAbsolutePath())) {
				
				Log.error("Error opening OSM Database. Quitting...");
				
				return;
			}
			
			pbf.getObjectCount();
			
			if (!pbf.process(db)) {
				
				Log.error("Error processing PBF File <"+pbfFiles[file].getAbsolutePath()+">. Quitting...");
				
				return;
			}
			
			db.saveDatabaseInfo(pbf);
			
			db.closeDatabase();
		}
		
		Log.info("Finished " + APP_NAME + "...");	
	}
}
