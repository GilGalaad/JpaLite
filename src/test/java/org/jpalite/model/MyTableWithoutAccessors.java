package org.jpalite.model;

import lombok.Getter;
import lombok.Setter;
import org.jpalite.annotation.Column;
import org.jpalite.annotation.Id;
import org.jpalite.annotation.Table;

import java.util.Date;

@Table(name = "my_table")
public class MyTableWithoutAccessors {

    @Id
    @Column(name = "my_key")
    @Getter
    private Long myKey;

    @Column(name = "string_col")
    @Getter
    private String stringCol;

    @Column(name = "int_col")
    @Setter
    private Integer intCol;

    @Column(name = "timestamp_col")
    @Setter
    private Date timestampCol;

}
