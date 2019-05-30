package org.gnuhpc.bigdata.service;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.ApiException;
import org.gnuhpc.bigdata.config.KafkaConfig;
import org.gnuhpc.bigdata.model.SchemaRegistryMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Service
public class ConfluentSchemaService {
  @Lazy
  @Autowired
  private KafkaConfig kafkaConfig;

  private KafkaAvroSerializer avroSerializer;
  private KafkaAvroSerializer avroSerializerForKey;
  private KafkaAvroDeserializer avroDeserializer;
  private KafkaAvroDeserializer avroDeserializerForKey;
  private CachedSchemaRegistryClient schemaRegistryClient;

  @PostConstruct
  public void init() {
    String schemaRegistryURL = kafkaConfig.getSchemaregistry();
    List<String> schemaRegistryURLList = new ArrayList<>();
    for (String url : schemaRegistryURL.split(",")) {
      schemaRegistryURLList.add(url);
    }

    int maxSchemaObject = AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT;
    this.schemaRegistryClient = new CachedSchemaRegistryClient(
        schemaRegistryURLList, maxSchemaObject);

    Map<String, String> propMap = new HashMap<>();
    propMap.put("schema.registry.url", schemaRegistryURL);

    avroSerializer = new KafkaAvroSerializer();
    avroSerializer.configure(propMap, false);
    avroSerializerForKey = new KafkaAvroSerializer();
    avroSerializerForKey.configure(propMap, true);

    avroDeserializer = new KafkaAvroDeserializer();
    avroDeserializer.configure(propMap, false);
    avroDeserializerForKey = new KafkaAvroDeserializer();
    avroDeserializerForKey.configure(propMap, true);
  }

  public byte[] serializeAvroToBytes(String topic, GenericRecord avroRecord) {
    return avroSerializer.serialize(topic, avroRecord);
  }

  public byte[] serializeAvroToBytesForKey(String topic, GenericRecord avroRecord) {
    return avroSerializerForKey.serialize(topic, avroRecord);
  }

  public Object deserializeBytesToObject(String topic, byte[] avroBytearray) {
    return avroDeserializer.deserialize(topic, avroBytearray);
  }

  public Object deserializeBytesToObjectForKey(String topic, byte[] avroBytearray) {
    return avroDeserializerForKey.deserialize(topic, avroBytearray);
  }

  public List<SchemaRegistryMetadata> getAllSubjects() {
    try {
      Collection<String> subjects = this.schemaRegistryClient.getAllSubjects();
      List<SchemaRegistryMetadata> allSubjects = new ArrayList<>();
      SchemaMetadata schemaMetadata;
      SchemaRegistryMetadata schemaRegistryMetadata;

      for (String subject : subjects) {
        schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
        schemaRegistryMetadata = SchemaRegistryMetadata.builder().subject(subject)
            .id(schemaMetadata.getId())
            .version(schemaMetadata.getVersion()).schema(schemaMetadata.getSchema()).build();
        allSubjects.add(schemaRegistryMetadata);
      }
      return allSubjects;
    } catch (Exception exception) {
      throw new ApiException("ConfluentSchemaService getAllSubjects exception : " + exception);
    }
  }

  public int registerSchema(String subject, String schemaStr) {
    int schemaId;
    Schema schema = null;
    try {
      schema = new Schema.Parser().parse(schemaStr);
      schemaId = schemaRegistryClient.register(subject, schema);
      return schemaId;
    } catch (Exception exception) {
      throw new ApiException(
          "ConfluentSchemaService registerSchema for subject:" + subject + " with schema:"
              + schemaStr + " exception : " + exception);
    }
  }

  public SchemaRegistryMetadata getSchemaById(int schemaId) {
    try {
      List<SchemaRegistryMetadata> allSubjects = getAllSubjects();
      List<SchemaRegistryMetadata> filteredSubjects = allSubjects.stream()
          .filter(p -> p.getId() == schemaId).collect(Collectors.toList());
      SchemaRegistryMetadata schemaRegistryMetadata =
          filteredSubjects.size() >= 1 ? filteredSubjects.get(0) : null;
      return schemaRegistryMetadata;
    } catch (Exception exception) {
      throw new ApiException(
          "ConfluentSchemaService getSchemaById for schemaId:" + schemaId + " exception : "
              + exception);
    }
  }

  public SchemaMetadata getSchemaBySubject(String subject) {
    try {
      SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
      return schemaMetadata;
    } catch (Exception exception) {
      throw new ApiException(
          "ConfluentSchemaService getSchemaBySubject for subject:" + subject + " exception : "
              + exception);
    }
  }

  public SchemaMetadata getSchemaBySubjectAndVersion(String subject, int version) {
    try {
      SchemaMetadata schemaMetadata = schemaRegistryClient.getSchemaMetadata(subject, version);
      return schemaMetadata;
    } catch (Exception exception) {
      throw new ApiException(
          "ConfluentSchemaService getSchemaBySubjectAndVersion for subject:" + subject
              + ", version:" + version + " exception : " + exception);
    }
  }

  public SchemaRegistryMetadata checkSchemaExist(String subject, String schemaStr) {
    try {
      Schema schema = new Schema.Parser().parse(schemaStr);
      int schemaId = schemaRegistryClient.getId(subject, schema);
      return getSchemaById(schemaId);
    } catch (Exception exception) {
      throw new ApiException(
          "ConfluentSchemaService checkSchemaExist for subject:" + subject
              + ", schemaStr:" + schemaStr + " exception : " + exception);
    }
  }

  public List<Integer> getAllVersions(String subject) {
    try {
      List<Integer> allVersions = schemaRegistryClient.getAllVersions(subject);
      return allVersions;
    } catch (Exception exception) {
      throw new ApiException(
          "ConfluentSchemaService getAllVersions for subject:" + subject + " exception : "
              + exception);
    }
  }

  public List<Integer> deleteSubject(String subject) {
    try {
      List<Integer> allDeletedVersions = schemaRegistryClient.deleteSubject(subject);
      return allDeletedVersions;
    } catch (Exception exception) {
      throw new ApiException(
          "ConfluentSchemaService deleteSubject for subject:" + subject + " exception : "
              + exception);
    }
  }
}
