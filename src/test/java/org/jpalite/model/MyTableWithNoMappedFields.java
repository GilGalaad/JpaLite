package org.jpalite.model;

import lombok.Data;
import org.jpalite.annotation.Id;
import org.jpalite.annotation.Table;

import java.util.Date;

@Data
@Table(name = "my_table")
public class MyTableWithNoMappedFields {

    @Id
    private Long myKey;

    private String unmappedField;

    private Integer intCol;

    private Date timestampCol;

}
