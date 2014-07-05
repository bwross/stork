package stork.module.irods;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.domain.UserFilePermission;
import org.irods.jargon.core.query.CollectionAndDataObjectListingEntry;

import stork.feather.Bell;
import stork.feather.Emitter;
import stork.feather.Path;
import stork.feather.Resource;
import stork.feather.Stat;
import stork.feather.Tap;

public class IRODSResource extends Resource<FeatherIRODSSession,IRODSResource>{


	protected IRODSResource(FeatherIRODSSession session, Path path) {
		super(session, path);
		// TODO Auto-generated constructor stub
	}

	public Emitter<String> list() {
		final Emitter<String> emitter = new Emitter<String>();
		stat().promise(new Bell<Stat>() {
			public void done(Stat s) {
				for (Stat ss : s.files)
					emitter.emit(ss.name);
				emitter.ring();
			} public void fail(Throwable t) {
				emitter.ring(t);
			}
		});
		return emitter;
	}
	
	public Bell<Stat> stat() {
		final Bell<Stat> bell = new Bell<Stat>();
		final String targetIrodsCollection = (null != path)?(path.toString()):null;
		initialize().promise(new Bell<IRODSResource>() {
			public void done() {
				new Thread(){
					public void run(){
						List<Stat> fileList = new ArrayList<Stat>();
				        List<CollectionAndDataObjectListingEntry> entries = null;
				        try {
				        	entries = session.actualCollection.listDataObjectsAndCollectionsUnderPathWithPermissions(targetIrodsCollection);
				        } catch (org.irods.jargon.core.exception.FileNotFoundException e1) {
				            e1.printStackTrace();
				            bell.ring(e1);
				        } catch (JargonException e1) {
				            e1.printStackTrace();
				            bell.ring(e1);
				        }
				        Iterator<CollectionAndDataObjectListingEntry> datacursor = entries.iterator();
				        while(datacursor.hasNext()){
				        	CollectionAndDataObjectListingEntry entry = datacursor.next();
				        	Stat fileinfo = new Stat();
				        	fileList.add(fileinfo);
				        	switch (entry.getObjectType()){
				        	case DATA_OBJECT:
				        		fileinfo.file = true;
				        		break;
				        	case COLLECTION:
				        		fileinfo.dir = true;
				        		break;
				        	default:
				        		break;
				        	}
				            fileinfo.name = entry.getNodeLabelDisplayValue();
				        	fileinfo.size = entry.getDataSize();
				        	fileinfo.time = entry.getModifiedAt().getTime();
				        	//System.out.println("owner: " + entry.getOwnerName());
				        	//fileinfo.setOwner(entry.getOwnerName());
				            //System.out.println("Ctime: " + entry.getCreatedAt());
				            //fileinfo.setDate(entry.getCreatedAt().toString());
				        	
				            List<UserFilePermission> permissionlist = entry.getUserFilePermission();
				            fileinfo.perm = permissionlist.toString();
				        }
			            Stat rootStat = new Stat(targetIrodsCollection);
			            rootStat.setFiles(fileList);
				        if(entries.isEmpty()){
				        	rootStat.file = true;
				        }else{
				        	rootStat.dir = true;
				        }
			            bell.ring(rootStat);
					}
				}.run();				
			} public void fail(Throwable t) {
				bell.ring(t);
			}
		});
 
        return bell;
	}
	
	 // Create a directory at the end-point, as well as any parent directories.
	public Bell<IRODSResource> mkdir() {
		return null;
	}

	// Remove a file or directory.
	public Bell<IRODSResource> unlink() {
		return null;
	}

	
	/*read*/
	public IRODSTap tap() {
	    return new IRODSTap(this);
	}
	/*write*/
	public IRODSSink sink() {
	    return new IRODSSink(this);
	}	
	
}
