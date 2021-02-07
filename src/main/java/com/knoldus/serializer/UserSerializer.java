package com.knoldus.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.knoldus.User;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class UserSerializer implements Serializer<User> {

    public void configure(Map<String, ?> map, boolean b) {
    }


    public byte[] serialize(String s, User o) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(o).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }


     public void close() {
    }
}