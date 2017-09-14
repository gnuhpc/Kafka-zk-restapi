package org.gnuhpc.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.constant.GeneralResponseState;

@Data
@Log4j
@AllArgsConstructor
public class GeneralResponse {
    private GeneralResponseState state;
    private String msg;
}
