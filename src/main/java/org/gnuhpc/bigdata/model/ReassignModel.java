package org.gnuhpc.bigdata.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.beans.ConstructorProperties;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/*
{
    "version": 1,
    "partitions": [
        {
            "topic": "first",
            "partition": 1,
            "replicas": [
                115
            ],
            "log_dirs": [
                "any"
            ]
        },
        {
            "topic": "first",
            "partition": 0,
            "replicas": [
                113
            ],
            "log_dirs": [
                "any"
            ]
        }
    ]
}
 */
@Getter
@Setter
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReassignModel {
  public int version = 1;
  public List<TopicPartitionReplicaAssignment> partitions;
}
