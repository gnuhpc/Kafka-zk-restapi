package org.gnuhpc.bigdata.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import lombok.Getter;
import lombok.Setter;
import org.joda.time.DateTime;

@Getter
@Setter
public class TimestampDeserializer extends JsonDeserializer<DateTime> {
  public TimestampDeserializer() {

  }

  @Override
  public DateTime deserialize(
      JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
    String timestamp = jp.getText().trim();

    try {
      return new DateTime(Long.valueOf(timestamp));
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
