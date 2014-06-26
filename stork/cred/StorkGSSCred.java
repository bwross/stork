package stork.cred;

import stork.ad.*;
import stork.feather.*;
import stork.util.*;

import java.util.*;
import java.io.*;

import org.globus.myproxy.*;
import org.ietf.jgss.*;
import org.gridforum.jgss.*;

// A wrapper for a GSS credential.

public class StorkGSSCred extends StorkCred<GSSCredential> {
  private String proxy_string = null;
  private int    proxy_life   = 3600;
  private String myproxy_user = null;
  private String myproxy_pass = null;
  private String myproxy_host = null;
  private int    myproxy_port = -1;
  private transient GSSCredential credential = null;

  StorkGSSCred() {
    super("gss-cred");
  }

  // Create a credential from MyProxy.
  public StorkGSSCred(URI u) {
    this(u.host(), u.port(), u.userPass());
  } public StorkGSSCred(String h, int i, String[] ui) {
    this(h, i, ui[0], ui[1]);
  } public StorkGSSCred(String h, int i, String u, String p) {
    this();
    myproxy_user = u;
    myproxy_pass = p;
    myproxy_host = h;
    myproxy_port = i;
    initialize();
  }

  // Create a credential from that horrible ill-defined certificate export
  // format specified in draft-ggf-gss-extensions-07 that Globus people like
  // to use for everything.
  public StorkGSSCred(String worthless_cred_junk) {
    this();
    proxy_string = worthless_cred_junk;
    initialize();
  }

  // Create a credential from either bytes or MyProxy.
  public StorkGSSCred(Ad ad) {
    this();
    ad.unmarshal(this);
    initialize();
  }

  // Lazily instantiate this credential.
  public GSSCredential data() {
    return (credential != null) ? credential : initialize();
  }

  // Call this after unmarshalling to instantiate credential from stored
  // information. Can be called again to refresh the credential as well.
  private GSSCredential initialize() {
    try {
      return credential = initCred();
    } catch (Exception e) {
      throw new RuntimeException(type+": "+e.getMessage());
    }
  } private GSSCredential initCred() throws Exception {
    if (proxy_life < 3600) {
      throw new Exception("cred lifetime must be at least one hour");
    } if (myproxy_user != null) {
      if (myproxy_port <= 0 || myproxy_port > 0xFFFF)
        myproxy_port = MyProxy.DEFAULT_PORT;
      MyProxy mp = new MyProxy(myproxy_host, myproxy_port);
      return mp.get(myproxy_user, myproxy_pass, proxy_life);
    } if (proxy_string != null) {
      // What the hell encoding does this thing expect? RFC 4462 seems
      // to suggest UTF-8, but who even knows.
      byte[] b = proxy_string.getBytes("UTF-8");
      ExtendedGSSManager gm =
        (ExtendedGSSManager) ExtendedGSSManager.getInstance();
      return gm.createCredential(
        b, ExtendedGSSCredential.IMPEXP_OPAQUE, GSSCredential.DEFAULT_LIFETIME, null,
        GSSCredential.INITIATE_AND_ACCEPT);
    } else {
      throw new Exception("not enough information");
    }
  }

  // Read a certificate from a local file.
  public static StorkGSSCred fromFile(String cred_file) {
    return fromFile(new File(cred_file));
  } public static StorkGSSCred fromFile(File cred_file) {
    return new StorkGSSCred(StorkUtil.readFile(cred_file));
  }

  // Create a new credential using MyProxy.
  public static StorkGSSCred fromMyProxy(String uri) {
    return fromMyProxy(URI.create(uri));
  } public static StorkGSSCred fromMyProxy(URI uri) {
    return new StorkGSSCred(uri);
  }
}
