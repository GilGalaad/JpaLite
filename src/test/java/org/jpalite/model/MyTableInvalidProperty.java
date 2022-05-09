package org.jpalite.model;

import lombok.Getter;
import lombok.Setter;
import org.jpalite.annotation.Column;
import org.jpalite.annotation.Id;
import org.jpalite.annotation.Table;

import java.util.Date;

@Table(name = "my_table")
public class MyTableInvalidProperty {

    @Id
    @Column(name = "my_key")
    @Getter
    @Setter
    private Long myKey;

    @Column(name = "string_col")
    @Getter
    @Setter
    private String stringCol;

    @Column(name = "int_col")
    @Getter
    @Setter
    private Integer intCol;

    @Column(name = "timestamp_col")
    public Date timestampCol;

}
