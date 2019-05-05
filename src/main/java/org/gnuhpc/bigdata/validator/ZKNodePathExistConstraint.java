package org.gnuhpc.bigdata.validator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Constraint(validatedBy = ZKNodePathExistValidator.class)
@Target( { ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface ZKNodePathExistConstraint {
  String message() default "Non-exist ZooKeeper Node path!";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};
}
