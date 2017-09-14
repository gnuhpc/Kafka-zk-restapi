package org.gnuhpc.bigdata.validator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.*;

@Documented
@Constraint(validatedBy = ConsumerGroupExistValidator.class)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface ConsumerGroupExistConstraint {
    String message() default "Non-exist Consumer Group!";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
