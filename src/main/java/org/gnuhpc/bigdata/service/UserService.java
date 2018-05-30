package org.gnuhpc.bigdata.service;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.config.WebSecurityConfig;
import org.gnuhpc.bigdata.constant.GeneralResponseState;
import org.gnuhpc.bigdata.model.GeneralResponse;
import org.gnuhpc.bigdata.model.User;
import org.gnuhpc.bigdata.utils.CommonUtils;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Getter
@Setter
@Log4j
@Service
public class UserService {
  private HashMap<Object, Object> accounts;

  public List<String> listUser() {
    List<String> userList = new ArrayList<>();
    try {
      accounts = CommonUtils.yamlParse(WebSecurityConfig.SECURITY_FILE_PATH);
      Iterator iterator = accounts.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry entry = (Map.Entry) iterator.next();
        String username = (String)entry.getKey();
        userList.add(username);
      }
    } catch (IOException ioException) {
      log.error("Failed to get user list. Reason : " + ioException.getLocalizedMessage());
    }

    return userList;
  }

  public GeneralResponse addUser(User user) {
    String username = user.getUsername();
    try {
      boolean exist = checkUserExist(username);
      if (!exist) {
        return saveUserInfo(user);
      } else {
        log.info("Failed to add user. Reason : User " + username + " already exists.");
        return new GeneralResponse(GeneralResponseState.failure, "Failed to add user. Reason : User " + username + " already exists.");
      }
    } catch (IOException ioException) {
      log.error("Failed to add user " + username + ". Reason : " + ioException.getLocalizedMessage());
      return new GeneralResponse(GeneralResponseState.failure,
              "Failed to add user " + username + ". Reason : " + ioException.getLocalizedMessage());
    }
  }

  public GeneralResponse modifyUser(User user) {
    String username = user.getUsername();
    try {
      boolean exist = checkUserExist(username);
      if (exist) {
        return saveUserInfo(user);
      } else {
        log.info("Failed to modify user. Reason : User " + username + " does not exist.");
        return new GeneralResponse(GeneralResponseState.failure, "Failed to modify user. Reason : User " + username + " does not exist.");
      }
    } catch (IOException ioException) {
      log.error("Failed to modify user " + username + ". Reason : " + ioException.getLocalizedMessage());
      return new GeneralResponse(GeneralResponseState.failure,
              "Failed to modify user " + username + ". Reason : " + ioException.getLocalizedMessage());
    }
  }

  public GeneralResponse delUser(String username) {
    try {
      boolean exist = checkUserExist(username);
      if (exist) {
        accounts.remove(username);
        CommonUtils.yamlWrite(WebSecurityConfig.SECURITY_FILE_PATH, accounts);
        return new GeneralResponse(GeneralResponseState.success, "Delete user " + username + " successfully.");
      } else {
        log.info("Failed to delete user. Reason : User " + username + " does not exist.");
        return new GeneralResponse(GeneralResponseState.failure, "Failed to delete user. Reason : User " + username + " does not exist.");
      }
    } catch (IOException ioException) {
      log.error("Failed to delete user " + username + ". Reason : " + ioException.getLocalizedMessage());
      return new GeneralResponse(GeneralResponseState.failure,
              "Failed to delete user " + username + ". Reason : " + ioException.getLocalizedMessage());
    }
  }

  public boolean checkUserExist(String username) throws IOException {
    accounts = CommonUtils.yamlParse(WebSecurityConfig.SECURITY_FILE_PATH);
    if (accounts.containsKey(username)) {
      return true;
    }
    return false;
  }

  public GeneralResponse saveUserInfo(User user) throws IOException {
    String username = user.getUsername();
    String encodedPassword = CommonUtils.encode(user.getPassword());
    HashMap<String, String> userInfo = new HashMap<>();

    userInfo.put("password", encodedPassword);
    userInfo.put("role", user.getRole());
    accounts.put(username, userInfo);
    CommonUtils.yamlWrite(WebSecurityConfig.SECURITY_FILE_PATH, accounts);
    return new GeneralResponse(GeneralResponseState.success, "Save user " + username + " info successfully.");
  }
}
