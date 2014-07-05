package stork.module.irods;

import stork.feather.*;
import stork.module.*;

public class IRODSModule extends Module<IRODSResource>{
	{ 
		name("Stork IRODS Module");
		protocols("irods");
		description("A module for interacting with iRODS servers.");
	}
	@Override
	public IRODSResource select(URI uri, Credential credential) {
		URI ep = uri.endpointURI();
		URI re = uri.resourceURI();
		return new FeatherIRODSSession(ep, credential).select(re.path());
	}
}
