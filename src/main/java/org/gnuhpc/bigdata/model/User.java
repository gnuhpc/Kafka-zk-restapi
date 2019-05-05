package org.gnuhpc.bigdata.model;

import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.NotBlank;

@Getter
@Setter
@AllArgsConstructor
public class User {

  @NotNull(message = "Username can not be null.")
  @NotBlank(message = "Username can not be blank.")
  private String username;

  @NotNull(message = "Password can not be null.")
  @NotBlank(message = "Password can not be blank.")
  private String password;

  @NotNull(message = "Role can not be null.")
  @NotBlank(message = "Role can not be blank.")
  private String role;
}
