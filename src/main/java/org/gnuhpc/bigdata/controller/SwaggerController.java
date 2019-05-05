package org.gnuhpc.bigdata.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import springfox.documentation.annotations.ApiIgnore;

@Controller
@ApiIgnore
public class SwaggerController {

  @GetMapping(value = "/api")
  public String swagger() {
    return "redirect:swagger-ui.html";
  }
}
