package dev.pipeline;

import java.util.Map;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/tasks")
public class TaskController {
    private final Pipeline engine;

    public TaskController(Pipeline engine) { this.engine = engine; }

    public record Submission(String asset, String revision, Long durationMs, Integer failStep,
                             Integer failTimes, Long timeoutMs) {}

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, String> submit(@RequestBody Submission submission) {
        return Map.of("taskId", engine.submit(submission));
    }

    @GetMapping
    public List<Map<String, Object>> list() { return engine.list(); }

    @GetMapping("/{id}")
    public Map<String, Object> detail(@PathVariable String id) { return engine.detail(id); }

    @GetMapping("/{id}/artifacts/{position}")
    public Object artifact(@PathVariable String id, @PathVariable int position) { return engine.artifact(id, position); }

    @PostMapping("/{id}/retry")
    public Map<String, Object> retry(@PathVariable String id) { engine.retry(id); return engine.detail(id); }

    @ExceptionHandler(IllegalArgumentException.class)
    public void invalid(IllegalArgumentException error) {
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, error.getMessage());
    }
}
