package org.jpalite.model;

import lombok.Data;
import org.jpalite.annotation.Column;
import org.jpalite.annotation.Id;
import org.jpalite.annotation.Table;

import java.util.Date;

@Data
@Table(name = "my_table")
public class MyTableWithAllId {

    @Id
    @Column(name = "my_key")
    private Long myKey;

    @Id
    @Column(name = "string_col")
    private String stringCol;

    @Id
    @Column(name = "int_col")
    private Integer intCol;

    @Id
    @Column(name = "timestamp_col")
    private Date timestampCol;

}
