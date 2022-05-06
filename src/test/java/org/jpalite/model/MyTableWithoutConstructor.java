package org.jpalite.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.jpalite.annotation.Column;
import org.jpalite.annotation.Id;
import org.jpalite.annotation.Table;

import java.util.Date;

@Data
@AllArgsConstructor
@Table(name = "my_table")
public class MyTableWithoutConstructor {

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
