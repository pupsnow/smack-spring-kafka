package com.smack.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Message {
    private String id;
    private String rate;
    private String description;

    @Override
    public String toString() {
        return "[" + id + " | " + rate + " | " + description + "]";
    }
}
