package org.gnuhpc.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.constant.GeneralResponseState;

@Data
@Log4j
@AllArgsConstructor
@Builder
public class GeneralResponse {
    private GeneralResponseState state;
    private String msg;
    private Object data;
}
