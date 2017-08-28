package org.gnuhpc.bigdata.validator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.*;

@Documented
@Constraint(validatedBy = TopicExistValidator.class)
@Target( { ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface TopicExistConstraint {
    String message() default "Non-exist Topic!";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}
