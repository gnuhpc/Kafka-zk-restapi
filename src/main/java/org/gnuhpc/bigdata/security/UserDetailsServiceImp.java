package org.gnuhpc.bigdata.security;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.config.WebSecurityConfig;
import org.gnuhpc.bigdata.model.User;
import org.gnuhpc.bigdata.utils.CommonUtils;
import org.springframework.security.core.userdetails.User.UserBuilder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Log4j
public class UserDetailsServiceImp implements UserDetailsService {
  private ScheduledExecutorService securityFileChecker;
  private int checkInitDelay;
  private int checkSecurityInterval;
  private ArrayList<User> userList = null;

  public UserDetailsServiceImp(boolean checkSecurity, int checkInitDelay, int checkSecurityInterval) {
    if (checkSecurity) {
      this.checkInitDelay = checkSecurityInterval;
      this.checkSecurityInterval = checkSecurityInterval;
      securityFileChecker = Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder().setNameFormat("securityFileChecker").build());
      securityFileChecker.scheduleWithFixedDelay(new SecurityFileCheckerRunnable(),
              checkInitDelay, checkSecurityInterval, TimeUnit.SECONDS);
      userList = fetchUserListFromSecurtiyFile();
    }
  }

  @Override
  public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
    User user = findUserByUsername(username);

    UserBuilder builder = null;
    if (user != null) {
      builder = org.springframework.security.core.userdetails.User.withUsername(username);
      builder.password(user.getPassword());
      builder.roles(user.getRole());
    } else {
      throw new UsernameNotFoundException("User not found.");
    }

    return builder.build();
  }

  private User findUserByUsername(String username) {
    for (User user:userList) {
      if (username.equals(user.getUsername())) {
        return user;
      }
    }
    return null;
  }

  private ArrayList<User> fetchUserListFromSecurtiyFile() {
    ArrayList userList = new ArrayList();
    String securityFilePath = WebSecurityConfig.SECURITY_FILE_PATH;
    try {
      HashMap<Object, Object> accounts = CommonUtils.yamlParse(securityFilePath);
      Iterator iter = accounts.entrySet().iterator();
      while (iter.hasNext()) {
        HashMap.Entry entry = (HashMap.Entry) iter.next();
        String username = (String)entry.getKey();
        Map<String, String> userInfo = (Map)entry.getValue();
        userList.add(new User(username, userInfo.get("password"), userInfo.get("role")));
      }
    } catch (IOException ioException) {
      log.error("Security file process exception.", ioException);
    }

    return userList;
  }

  private class SecurityFileCheckerRunnable implements Runnable {
    @Override
    public void run() {
      try {
        userList = fetchUserListFromSecurtiyFile();
      } catch (Throwable t) {
        log.error("Uncaught exception in SecurityFileChecker thread", t);
      }
    }
  }
}
