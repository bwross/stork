package stork.cred;

import stork.ad.*;
import stork.feather.*;

// A username and password credential.

public class StorkUserinfo extends StorkCred<String[]> {
  public String[] userpass;

  public StorkUserinfo(Ad ad) {
    this(ad.get("user"), ad.get("pass"));
  } public StorkUserinfo(String user, String pass) {
    this(new String[] { user, pass });
  } public StorkUserinfo(String userinfo) {
    this(split(userinfo));
  } private StorkUserinfo(String[] ui) {
    super("userinfo");
    userpass = ui;
  }

  public String type() {
    return "userinfo";
  }

  public String[] data() {
    return userpass;
  }

  // Return a user/pass pair from a colon-separated string.
  public static String[] split(URI uri) {
    return split(uri.userInfo());
  } public static String[] split(String ui) {
    String u = null, p = null;
    if (ui != null && !ui.isEmpty()) {
      int i = ui.indexOf(':');
      u = (i < 0) ? ui : ui.substring(0,i);
      p = (i < 0) ? "" : ui.substring(i+1);
    } return new String[] { u, p };
  }

  public String getUser() {
    return userpass[0];
  }

  public String getPass() {
    return userpass[1];
  }
}
