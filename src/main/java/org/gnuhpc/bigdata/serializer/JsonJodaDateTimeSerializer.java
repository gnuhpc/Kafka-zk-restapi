package org.gnuhpc.bigdata.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/** Created by gnuhpc on 2017/7/19. */
public class JsonJodaDateTimeSerializer extends JsonSerializer<DateTime> {

  private static DateTimeFormatter formatter = ISODateTimeFormat.dateTime();

  @Override
  public void serialize(
      DateTime value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {
    jsonGenerator.writeString(formatter.print(value));
  }
}
