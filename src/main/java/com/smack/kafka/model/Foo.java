package com.smack.kafka.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class Foo {

    private String value;

    public Foo(String foo) {
        this.value = foo;
    }

    @Override
    public String toString() {
        return "[value = " + value + "]";
    }
}
