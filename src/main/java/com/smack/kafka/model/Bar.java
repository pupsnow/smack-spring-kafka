package com.smack.kafka.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Setter
@Getter
public class Bar {

    private String value;

    public Bar(String bar) {
        this.value = bar;
    }

    @Override
    public String toString() {
        return "[value = " + this.value + "]";
    }
}
