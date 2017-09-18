package org.gnuhpc.bigdata.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.gnuhpc.bigdata.serializer.JsonJodaDateTimeSerializer;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@ToString
public class BrokerInfo
{
    @SerializedName("listener_security_protocol_map")
    private Map securityProtocol;
    @SerializedName("endpoints")
    private List<String> endPoints;
    @SerializedName("jmx_port")
    private int jmxPort;
    private String host;

    @JsonSerialize(using = JsonJodaDateTimeSerializer.class)
    @SerializedName("timestamp")
    private DateTime startTime;

    private int port;
    private int version;
    private String rack = "";
    private int id= -1;
}
