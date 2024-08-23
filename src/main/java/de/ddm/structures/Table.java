package de.ddm.structures;

import lombok.Data;

import java.util.List;

/**
 * @author timo.buechert
 */
@Data
public class Table {

    private int id;
    private List<String> header;
    private List<List<String>> data;

}
