package org.gnuhpc.bigdata.task;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FetchOffSetFromZKResult {
    private String topic;
    private int parition;
    private long offset;
}
