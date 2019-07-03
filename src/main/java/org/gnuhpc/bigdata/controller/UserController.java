package org.gnuhpc.bigdata.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.List;
import javax.validation.Valid;
import lombok.extern.log4j.Log4j2;
import org.gnuhpc.bigdata.constant.GeneralResponseState;
import org.gnuhpc.bigdata.model.GeneralResponse;
import org.gnuhpc.bigdata.model.User;
import org.gnuhpc.bigdata.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Log4j2
@RestController
@Api(value = "/users", description = "Security User Management Controller.")
public class UserController {

  @Autowired private UserService userService;

  @GetMapping("/users")
  @ApiOperation(value = "Get user list.")
  public List<String> listUser() {
    return userService.listUser();
  }

  @PostMapping("/users")
  @ApiOperation(value = "Add user.")
  public GeneralResponse addUser(@RequestBody @Valid User user, BindingResult results) {
    if (results.hasErrors()) {
      return GeneralResponse.builder()
          .state(GeneralResponseState.failure)
          .msg(results.getFieldError().getDefaultMessage())
          .build();
    }
    log.info("Receive add user request: username:" + user.getUsername());
    return userService.addUser(user);
  }

  @PutMapping("/users")
  @ApiOperation(value = "Modify user information.")
  public GeneralResponse modifyUser(@RequestBody @Valid User user, BindingResult results) {
    if (results.hasErrors()) {
      return GeneralResponse.builder()
          .state(GeneralResponseState.failure)
          .msg(results.getFieldError().getDefaultMessage())
          .build();
    }
    log.info("Receive modify user request: username:" + user.getUsername());
    return userService.modifyUser(user);
  }

  @DeleteMapping("/users/{username}")
  @ApiOperation(value = "Delete user.")
  public GeneralResponse delUser(@PathVariable String username) {
    log.info("Receive delete user request: username:" + username);
    return userService.delUser(username);
  }
}
