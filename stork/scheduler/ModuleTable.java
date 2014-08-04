package stork.scheduler;

import stork.ad.*;
import stork.module.*;
import stork.util.*;
import static stork.util.StorkUtil.join;

import java.util.*;
import java.io.*;

// A class for looking up transfer modules by protocol and handle.

public class ModuleTable {
  private static ModuleTable instance = null;
  Map<String, Module> by_proto, by_handle;

  public ModuleTable() {
    by_proto  = new HashMap<String, Module>();
    by_handle = new HashMap<String, Module>();
  }

  // Get the global transfer module table.
  public static synchronized ModuleTable instance() {
    if (instance == null)
      instance = new ModuleTable();
    return instance;
  }

  // Add a directory of executables to the transfer module table.
  // TODO: Do this in parallel and detect misbehaving externals.
  public void registerDirectory(File dir) {
    if (!dir.isDirectory()) {
      Log.warning('"', dir, "\" is not a directory!");
    } else for (File f : dir.listFiles()) {
      // Skip over things that obviously aren't transfer modules.
      if (f.isFile() && !f.isHidden() && f.canExecute())
        register(f);
    }
  }

  // Add a transfer module to the table.
  public void register(File f) {
    //register(new ExternalModule(f));
  } public void register(Module m) {
    // Check if handle is in use.
    if (!by_handle.containsKey(m.handle())) {
      by_handle.put(m.handle(), m);
      Log.info("Registered module \"", m, "\" [handle: ", m.handle(), "]");
    } else {
      Log.warning("Module handle \"", m.handle(), "\"in use, ignoring");
      return;
    }

    // Add the protocols for this module.
    Set<String> good = new TreeSet<String>(), bad = new TreeSet<String>();
    for (String p : m.protocols()) {
      if (by_proto.get(p) == null) {
        good.add(p);
        by_proto.put(p, m);
      } else {
        bad.add(p);
      }
    }

    if (!good.isEmpty())
      Log.info("  Registering protocol(s): ", join(good.toArray()));
    if (!bad.isEmpty())
      Log.info("  Protocols already registered: ", join(bad.toArray()));
  }

  // Get all of the transfer module info ads in a list.
  public Ad infoAds() {
    Ad mods = new Ad();
    for (Module m : by_handle.values())
      mods.put(m.handle(), Ad.marshal(m));
    return mods;
  }

  // Get a transfer module by its handle.
  public Module byHandle(String h) {
    Module m = by_handle.get(h);
    if (m != null)
      return m;
    throw new RuntimeException("no module '"+h+"' registered");
  }

  // Get a transfer module by protocol.
  public Module byProtocol(String p) {
    Module m = by_proto.get(p);
    if (m != null)
      return m;
    throw new RuntimeException("no module for protocol '"+p+"' registered");
  }

  // Get a set of all the modules.
  public Collection<Module> modules() {
    return by_handle.values();
  }

  // Get a set of all the handles.
  public Collection<String> handles() {
    return by_handle.keySet();
  }

  // Get a set of all the supported protocols.
  public Collection<String> protocols() {
    return by_proto.keySet();
  }
}
