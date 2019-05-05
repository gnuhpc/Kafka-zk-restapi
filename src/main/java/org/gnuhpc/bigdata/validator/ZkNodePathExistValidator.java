package org.gnuhpc.bigdata.validator;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.springframework.beans.factory.annotation.Autowired;

public class ZkNodePathExistValidator
    implements ConstraintValidator<ZkNodePathExistConstraint, String> {

  @Autowired private ZookeeperUtils zookeeperUtils;

  public void initialize(ZkNodePathExistConstraint constraint) {}

  public boolean isValid(String path, ConstraintValidatorContext context) {
    return (zookeeperUtils.getNodePathStat(path) != null);
  }
}
