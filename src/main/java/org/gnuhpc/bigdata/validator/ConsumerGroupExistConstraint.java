package org.gnuhpc.bigdata.validator;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.Payload;

@Documented
@Constraint(validatedBy = ConsumerGroupExistValidator.class)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface ConsumerGroupExistConstraint {

  String message() default "Non-exist Consumer Group!";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};
}
