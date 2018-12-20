package org.gnuhpc.bigdata.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.constant.GeneralResponseState;
import org.gnuhpc.bigdata.model.GeneralResponse;
import org.gnuhpc.bigdata.model.User;
import org.gnuhpc.bigdata.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

@Log4j
@RestController
@Api(value = "/users", description = "Security User Management Controller.")
public class UserController {
  @Autowired
  private UserService userService;

  @GetMapping("/users")
  @ApiOperation(value = "Get user list.")
  public List<String> listUser() {
    return userService.listUser();
  }

  @PostMapping("/users")
  @ApiOperation(value = "Add user.")
  public GeneralResponse addUser(@RequestBody@Valid User user, BindingResult results) {
    if (results.hasErrors()) {
      return GeneralResponse.builder().state(GeneralResponseState.failure).msg(results.getFieldError().getDefaultMessage()).build();
    }
    log.info("Receive add user request: username:" + user.getUsername());
    return userService.addUser(user);
  }

  @PutMapping("/users")
  @ApiOperation(value = "Modify user information.")
  public GeneralResponse modifyUser(@RequestBody@Valid User user, BindingResult results) {
    if (results.hasErrors()) {
      return GeneralResponse.builder().state(GeneralResponseState.failure).msg(results.getFieldError().getDefaultMessage()).build();
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
