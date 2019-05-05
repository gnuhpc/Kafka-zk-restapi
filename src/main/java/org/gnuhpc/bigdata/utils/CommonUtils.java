package org.gnuhpc.bigdata.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

@Log4j
@Getter
@Setter
public class CommonUtils {

  public static final String PROJECT_ROOT_FOLDER = CommonUtils.getProjectRootPath();

  public static String encode(CharSequence rawPassword) {
    return new BCryptPasswordEncoder().encode(rawPassword);
  }

  public static String getProjectRootPath() {
    String workingDir = System.getProperty("user.dir");
    log.info("Current working directory : " + workingDir);
    return workingDir;
  }

  public static HashMap<Object, Object> yamlParse(String filePath) throws IOException {
    ObjectMapper mapperForYaml = new ObjectMapper(new YAMLFactory());
    File file = new File(filePath);
    HashMap<Object, Object> yamlHash = new HashMap<>();
    yamlHash = mapperForYaml.readValue(file, yamlHash.getClass());

    return yamlHash;
  }

  public static HashMap<Object, Object> yamlParse(File file) throws IOException {
    ObjectMapper mapperForYaml = new ObjectMapper(new YAMLFactory());
    HashMap<Object, Object> yamlHash = new HashMap<>();
    yamlHash = mapperForYaml.readValue(file, yamlHash.getClass());

    return yamlHash;
  }

  public static void yamlWrite(String filePath, Object object) throws IOException {
    File file = new File(filePath);
    ObjectMapper mapperForYaml = new ObjectMapper(new YAMLFactory());
    mapperForYaml.writeValue(file, object);
  }

  public static void yamlWrite(File file, Object object) throws IOException {
    ObjectMapper mapperForYaml = new ObjectMapper(new YAMLFactory());
    mapperForYaml.writeValue(file, object);
  }

  public static void main(String[] args) throws IOException {
    /*
    String rawPassword = "admin";
    String encodedPassword = CommonUtils.encode(rawPassword);
    System.out.println("rawPassword:" + rawPassword + ", encodedPassword:" + encodedPassword);
    System.out.println("workingDir:" + CommonUtils.PROJECT_ROOT_FOLDER);
    */
  }
}
