package org.gnuhpc.bigdata.validator;

import org.gnuhpc.bigdata.service.KafkaAdminService;
import org.springframework.beans.factory.annotation.Autowired;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class TopicExistValidator implements ConstraintValidator<TopicExistConstraint, String> {
    @Autowired
    private KafkaAdminService kafkaAdminService;
    public void initialize(TopicExistConstraint constraint) {
    }

    public boolean isValid(String topic, ConstraintValidatorContext context) {
        return kafkaAdminService.existTopic(topic);
    }
}
