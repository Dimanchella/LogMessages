package ru.dim.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LogMessage {
    private String app;
    private String message;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private LocalDateTime date;
    private String logger;
}
