package stork.cred;

import stork.ad.*;
import stork.feather.*;
import stork.scheduler.*;
import stork.util.*;

// An extended Feather credential with additional metadata.

public abstract class StorkCred<O> extends Credential<O> {
  public final String type;
  public String name;

  public StorkCred(String type) {
    this(null, type);
  } public StorkCred(String name, String type) {
    this.name = name;
    this.type = type;
  }

  // Retrieve a credential by UUID.
  public static StorkCred unmarshal(String uuid) {
    return Scheduler.instance().creds.getCred(uuid);
  }

  // Get or set the name.
  public final String name() {
    if (name == null)
      return "(unnamed)";
    return name;
  } public StorkCred<O> name(String s) {
    name = s;
    return this;
  }

  // Get an ad suitable for showing to users. It should not include sensitive
  // information.
  public Ad getAd() {
    return new Ad("name", name)
             .put("type", type);
  }
}
