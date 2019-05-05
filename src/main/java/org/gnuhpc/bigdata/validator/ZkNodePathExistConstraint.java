package org.gnuhpc.bigdata.validator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.Payload;

@Constraint(validatedBy = ZkNodePathExistValidator.class)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface ZkNodePathExistConstraint {

  String message() default "Non-exist ZooKeeper Node path!";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};
}
