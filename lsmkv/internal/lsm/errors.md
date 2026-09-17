# Error Names

Standardized error identifiers used across the engine.

| Error name           | Meaning                                               | Used starting |
|----------------------|-------------------------------------------------------|----------------|
| `InvalidArgument`    | Caller passed something illegal (e.g. empty key)      | Section 0      |
| `StoreClosed`        | Operation attempted after `Close()` was called        | Section 0      |
| `IOFailure`          | Underlying file/disk operation failed                 | Later          |
| `CorruptionDetected` | Data on disk failed an integrity check                | Future         |
| `NotImplemented`     | Stub response — feature not built yet                 | Section 0 (allowed) |