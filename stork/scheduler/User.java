package stork.scheduler;

import stork.Stork;
import stork.ad.*;
import stork.feather.*;
import stork.cred.*;
import stork.util.*;

import java.security.*;
import java.util.*;

// A user in the StorkCloud system. Each user has their own view of the job
// queue, transfer credential manager, login credentials, and user info.
// Each user also has an associated UUID which serves as a unique identifier
// that never changes.

public class User {
  public String email;
  public String hash;
  public String salt;
  public String name;

  public ArrayList<Job> jobs = new ArrayList<Job>();
  public LinkedList<URI> history = new LinkedList<URI>();
  public HashSet<String> creds = new HashSet<String>();

  // The minimum password length.
  public static final int PASS_LEN = 6;

  // Create an anonymous user. Only instantiate one of these, and do not
  // register it in any UserMap.
  private User() { 
    email = "anonymous";
  }

  // Create a user from an ad.
  public User(Ad ad) {
    this(ad.get("email"));

    ad.unmarshal(this);

    if (ad.has("hash") && ad.has("salt")) {
      // Do nothing.
    } else if (ad.has("password")) {
      setPassword(ad.get("password"));
    }
  }

  // Create a user with the given email.
  public User(String email) {
    try {
      //InternetAddress ia = new InternetAddress(email, true);
      //email = ia.getAddress();
      this.email = email;
    } catch (Exception e) {
      throw new RuntimeException("Invalid email address provided.");
    }
  }

  // A class which can be used to store users.
  public static class Map extends HashMap<String, User> {
    // Add a user to this user map based on a registration ad.
    public synchronized User register(Ad ad) {
      // Filter some stuff we don't want from users.
      ad.remove("jobs", "creds");

      User su = new User(ad);
      su.setPassword(ad.get("password"));

      // If the user already exists, try logging in.
      if (lookup(su.email) != null) try {
        return login(ad);
      } catch (Exception e) {
        throw new RuntimeException("This email is already in use.");
      } return insert(su);
    }

    // Check a login request. Returns the user object if the login was
    // successful. Throws an exception if there's a problem.
    public User login(Ad ad) {
      // Get information from the login ad.
      String email;
      try {
        email = ad.get("email").trim().toLowerCase();
        if (email.isEmpty()) throw null;
      } catch (Exception e) {
        throw new RuntimeException("No email address provided.");
      }

      String pass = ad.get("password");
      String hash = ad.get("hash");

      if (pass == null && hash == null)
        throw new RuntimeException("No password provided.");

      // Check if the user is in the system.
      User su = lookup(email);

      if (su == null)
        throw new RuntimeException("Invalid username or password.");
      if (su.hash == null)
        throw new RuntimeException("User account is not verified.");

      // If a hash was provided, check it first.
      if (hash != null && hash.equals(su.hash))
        return su;

      // Otherwise, check the password.
      if (pass != null && su.hash(pass).equals(su.hash))
        return su;

      throw new RuntimeException("Invalid username or password.");
    }

    // Put a user into the map, using the normalized email as the key.
    public User insert(User su) {
      put(su.email.trim().toLowerCase().replace('.', '_'), su);
      return su;
    }

    // Lookup a Stork user by user id. Returns null if the user does
    // not exist.
    public User lookup(String id) {
      return get(id.trim().toLowerCase().replace('.', '_'));
    }
  }

  // Set the password for this user. Checks password length and handles
  // hashing and whatnot. Throws if there's an error.
  public synchronized void setPassword(String pass) {
    if (pass == null || pass.isEmpty())
      throw new RuntimeException("No password was provided.");
    if (pass.length() < PASS_LEN)
      throw new RuntimeException("Password shorter than "+PASS_LEN+" characters.");

    salt = salt();
    hash = hash(pass);
  }

  // Get an ad to return to the user on login.
  public Ad toAd() {
    return Ad.marshal(this).filter("email", "hash", "history");
  }

  // Add a URL to a user's history. Keep the history limited to the
  // configured maximum.
  public synchronized void addHistory(URI u) {
    if (!isAnonymous() && Stork.settings.max_history > 0) try {
      history.remove(u);
      while (history.size() > Stork.settings.max_history)
        history.removeLast();
      history.addFirst(u);
    } catch (Exception e) {
      // Just don't add it.
    }
  }

  // Return the display name of the user. Either their full name or
  // their user id.
  public String name() {
    return (name != null) ? name : email;
  }

  // Get an anonymous user.
  public static User anonymous() {
    return new User() {
      public boolean isAnonymous() {
        return true;
      }
    };
  }

  // The anonymous user overrides this.
  public boolean isAnonymous() {
    return false;
  }

  // Generate a random salt using a secure random number generator.
  public static String salt() {
    return salt(24);
  } public static String salt(int len) {
    byte[] b = new byte[len];
    SecureRandom random = new SecureRandom();
    random.nextBytes(b);
    return StorkUtil.formatBytes(b, "%02x");
  }

  // Hash a password using salt.
  // TODO: Check password constraints.
  public String hash(String pass) {
    return hash(pass, salt);
  } public static String hash(String pass, String salt) {
    try {
      String saltpass = salt+'\n'+pass;
      MessageDigest md = MessageDigest.getInstance("SHA-1");
      byte[] digest = saltpass.getBytes("UTF-8");

      // Run the digest for two rounds.
      for (int i = 0; i < 2; i++)
        digest = md.digest(digest);

      return StorkUtil.formatBytes(digest, "%02x");
    } catch (Exception e) {
      throw new RuntimeException("Couldn't hash password.");
    }
  }
}
