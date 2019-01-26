package org.gnuhpc.bigdata.model;

import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
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
@Builder
public class ReassignModel {
  public final int version = 1;
  public List<TopicPartitionReplicaAssignment> partitions;
}
