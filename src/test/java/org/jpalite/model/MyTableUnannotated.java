package org.jpalite.model;

import lombok.Data;
import org.jpalite.annotation.Column;
import org.jpalite.annotation.Id;

import java.util.Date;

@Data
public class MyTableUnannotated {

    @Id
    @Column(name = "my_key")
    private Long myKey;

    @Column(name = "string_col")
    private String stringCol;

    @Column(name = "int_col")
    private Integer intCol;

    @Column(name = "timestamp_col")
    private Date timestampCol;

}
