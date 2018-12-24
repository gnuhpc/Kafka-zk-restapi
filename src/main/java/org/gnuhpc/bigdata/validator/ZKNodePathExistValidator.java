package org.gnuhpc.bigdata.validator;

import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class ZKNodePathExistValidator implements ConstraintValidator<ZKNodePathExistConstraint, String> {
  @Autowired
  private ZookeeperUtils zookeeperUtils;

  public void initialize(ZKNodePathExistConstraint constraint) {
  }

  public boolean isValid(String path, ConstraintValidatorContext context) {
    return (zookeeperUtils.getNodePathStat(path)!=null);
  }
}
