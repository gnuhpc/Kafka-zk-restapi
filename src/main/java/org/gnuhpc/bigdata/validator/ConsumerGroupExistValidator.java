package org.gnuhpc.bigdata.validator;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import org.gnuhpc.bigdata.service.KafkaAdminService;
import org.springframework.beans.factory.annotation.Autowired;

public class ConsumerGroupExistValidator
    implements ConstraintValidator<ConsumerGroupExistConstraint, String> {

  @Autowired private KafkaAdminService kafkaAdminService;

  public void initialize(ConsumerGroupExistConstraint constraint) {}

  public boolean isValid(String consumerGroup, ConstraintValidatorContext context) {
    return kafkaAdminService.isNewConsumerGroup(consumerGroup)
        || kafkaAdminService.isOldConsumerGroup(consumerGroup);
  }
}
