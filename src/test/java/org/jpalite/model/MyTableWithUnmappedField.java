package org.jpalite.model;

import lombok.Data;
import org.jpalite.annotation.Column;
import org.jpalite.annotation.Id;
import org.jpalite.annotation.Table;

import java.util.Date;

@Data
@Table(name = "my_table")
public class MyTableWithUnmappedField {

    @Id
    @Column(name = "my_key")
    private Long myKey;

    private String unmappedField;

    @Column(name = "int_col")
    private Integer intCol;

    @Column(name = "timestamp_col")
    private Date timestampCol;

}
