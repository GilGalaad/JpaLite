package org.jpalite.model;

import lombok.Getter;
import org.jpalite.annotation.Column;
import org.jpalite.annotation.Id;
import org.jpalite.annotation.Table;

import java.util.Date;

@Table(name = "my_table")
public class MyTableWithoutSetter {

    @Id
    @Column(name = "my_key")
    @Getter
    private Long myKey;

    @Column(name = "string_col")
    @Getter
    private String stringCol;

    @Column(name = "int_col")
    @Getter
    private Integer intCol;

    @Column(name = "timestamp_col")
    @Getter
    private Date timestampCol;

}
