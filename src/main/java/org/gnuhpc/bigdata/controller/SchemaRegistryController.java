package org.gnuhpc.bigdata.controller;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.List;
import org.gnuhpc.bigdata.model.SchemaRegistryMetadata;
import org.gnuhpc.bigdata.service.ConfluentSchemaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/schemaregistry")
@Api(value = "Control schema registry with Rest API")
public class SchemaRegistryController {

  @Lazy
  @Autowired
  private ConfluentSchemaService confluentSchemaService;

  @GetMapping("/schemas/ids/{schemaId}")
  @ApiOperation(value = "Get schema by id")
  public SchemaRegistryMetadata getSchemaById(@PathVariable int schemaId) {
    return confluentSchemaService.getSchemaById(schemaId);
  }

  @GetMapping("/subjects")
  @ApiOperation(value = "List all subjects")
  public List<SchemaRegistryMetadata> lsAllSubjects() {
    return confluentSchemaService.getAllSubjects();
  }

  @GetMapping("/subjects/{subject}")
  @ApiOperation(value = "Get latest schema by subject")
  public SchemaMetadata getSchemaBySubject(@PathVariable String subject) {
    return confluentSchemaService.getSchemaBySubject(subject);
  }

  @PostMapping("/subjects/{subject}/versions")
  @ApiOperation(value = "Get latest schema by subject")
  public int registerSchema(@PathVariable String subject, @RequestParam String schemaStr) {
    return confluentSchemaService.registerSchema(subject, schemaStr);
  }

  @PostMapping("/subjects/{subject}")
  @ApiOperation(value = "Check if a schema has already been registered under the specified subject")
  public SchemaRegistryMetadata checkSchemaExist(@PathVariable String subject,
      @RequestParam String schemaStr) {
    return confluentSchemaService.checkSchemaExist(subject, schemaStr);
  }

  @DeleteMapping("/subjects/{subject}")
  @ApiOperation(value = "Delete the specified subject and its associated compatibility level if "
      + "registered.")
  public List<Integer> deleteSubject(@PathVariable String subject) {
    return confluentSchemaService.deleteSubject(subject);
  }

  @GetMapping("/subjects/{subject}/versions/{versionId}")
  @ApiOperation(value = "Get schema by subject and version")
  public SchemaMetadata getSchemaBySubjectAndVersion(@PathVariable String subject,
      @PathVariable int versionId) {
    return confluentSchemaService.getSchemaBySubjectAndVersion(subject, versionId);
  }

  @GetMapping("/subjects/{subject}/versions")
  @ApiOperation(value = "Get all versions for the specified subject")
  public List<Integer> getAllVersionsBySubject(@PathVariable String subject) {
    return confluentSchemaService.getAllVersions(subject);
  }


}
