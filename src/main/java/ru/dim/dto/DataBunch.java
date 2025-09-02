package ru.dim.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.stream.Stream;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataBunch {
    private long bunchNumber;
    private Stream<String> dataLines;
}
