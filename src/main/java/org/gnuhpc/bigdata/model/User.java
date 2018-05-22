package org.gnuhpc.bigdata.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class User {
  private String username;
  private String password;
  private String role;

  public User(String username, String password, String role) {
    this.username = username;
    this.password = password;
    this.role = role;
  }
}
