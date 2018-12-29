/*
 * Copyright (c) 2016 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gnuhpc.bigdata.model;

import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class TopicDetail {

  public static final int DEFAULT_PARTITION_NUMBER = 2;
  public static final int DEFAULT_REPLICATION_FACTOR = 1;

  private int partitions;
  private int factor;
  private String name;
  private Properties prop;

  public TopicDetail() {
    this.partitions = DEFAULT_PARTITION_NUMBER;
    this.factor = DEFAULT_REPLICATION_FACTOR;
    this.prop = new Properties();
  }
}
