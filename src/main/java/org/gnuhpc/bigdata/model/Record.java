package org.gnuhpc.bigdata.model;

import java.nio.ByteBuffer;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.utils.Bytes;

@Data
@Getter
@Setter
@Builder
public class Record {
  public String topic;
  public long offset;
  public Object key = new Object();
  public Object value = new Object();
  public long timestamp;
  public Class<?> type;

  public String getValue() {
    try {
      if (String.class.isAssignableFrom(type)) {
        return value.toString();
      }

      if (Short.class.isAssignableFrom(type)) {
        return value.toString();
      }

      if (Integer.class.isAssignableFrom(type)) {
        return value.toString();
      }

      if (Long.class.isAssignableFrom(type)) {
        return value.toString();
      }

      if (Float.class.isAssignableFrom(type)) {
        return value.toString();
      }

      if (Double.class.isAssignableFrom(type)) {
        return value.toString();
      }

      if (Bytes.class.isAssignableFrom(type)) {
        Bytes bytes = (Bytes) value;
        return bytes.toString();
      }

      if (byte[].class.isAssignableFrom(type)) {
        byte[] byteArray = (byte[]) value;
        return new String(byteArray);
      }

      if (ByteBuffer.class.isAssignableFrom(type)) {
        ByteBuffer byteBuffer = (ByteBuffer) value;
        return new String(byteBuffer.array());
      }
    } catch (Exception exception) {
      throw new ApiException("Record Cast exception." + exception);
    }

    throw new ApiException(
        "Unknown class. Supported types are: "
            + "String, Short, Integer, Long, Float, Double, ByteArray, ByteBuffer, Bytes");
  }

  @Override
  public String toString() {
    if (value != null) {
      return "topic:"
          + topic
          + ", offset:"
          + offset
          + ", key:"
          + key
          + ", value:"
          + getValue()
          + ", timestamp:"
          + timestamp
          + ", type:"
          + type;
    } else {
      return "topic:"
          + topic
          + ", offset:"
          + offset
          + ", key:"
          + key
          + ", value:"
          + value
          + ", timestamp:"
          + timestamp
          + ", type:"
          + type;
    }
  }
}
