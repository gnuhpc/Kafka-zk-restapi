package org.gnuhpc.bigdata.service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.model.User;
import org.springframework.security.core.userdetails.User.UserBuilder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Log4j
public class UserDetailsServiceImp implements UserDetailsService {
  private ScheduledExecutorService securityFileChecker;
  private int checkInitDelay = 30;
  private int checkSecurityInterval = 60;
  private ArrayList<User> userList = null;

  public UserDetailsServiceImp() {
    securityFileChecker = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("securityFileChecker").build());
    securityFileChecker.scheduleWithFixedDelay(new securityFileCheckerRunnable(),
            checkInitDelay, checkSecurityInterval, TimeUnit.SECONDS);
    userList = fetchUserListFromSecurtiyFile();
  }

  @Override
  public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
    User user = findUserByUsername(username);

    UserBuilder builder = null;
    if (user != null) {
      builder = org.springframework.security.core.userdetails.User.withUsername(username);
      builder.password(new BCryptPasswordEncoder().encode(user.getPassword()));
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
    File tempFile = new File("");
    String securityFilePath = "";
    try {
      String projectRootPath = tempFile.getCanonicalPath();
      securityFilePath = projectRootPath + File.separator + "security/securityFile.txt";
      FileInputStream inputStream = new FileInputStream(securityFilePath);
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
      String usernameAndPwd;
      int lineNum = 0;
      while((usernameAndPwd = bufferedReader.readLine()) != null)
      {
        lineNum++;
        String[] strArray = usernameAndPwd.split(":");
        if (strArray.length < 3) {
          log.error("Security file:" + securityFilePath + ",line " + lineNum + " is " +
                  usernameAndPwd + ". The correct format should be username:passwd:role.");
        } else {
          String usernameInFile = strArray[0];
          String pwdInFile = strArray[1];
          String userRole = strArray[2];
          userList.add(new User(usernameInFile, pwdInFile, userRole));
        }
      }
      inputStream.close();
      bufferedReader.close();
    } catch (FileNotFoundException fileNotFoundException) {
      log.error("Security file in path: " + securityFilePath + " does not exist.", fileNotFoundException);
    } catch (IOException ioException) {
      log.error("Security file process exception.", ioException);
    }
    return userList;
  }

  private class securityFileCheckerRunnable implements Runnable {
    @Override
    public void run() {
      try {
        userList = fetchUserListFromSecurtiyFile();
      } catch (Throwable t) {
        log.error("Uncaught exception in securityFileChecker thread", t);
      }
    }
  }
}
