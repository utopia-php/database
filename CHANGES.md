# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [6.0.0] - 2026-03-25

### Added
- `Database::EVENT_CACHE_READ_FAILURE` event constant, emitted when a cache load fails during a document read. Previously these failures incorrectly emitted `EVENT_CACHE_PURGE_FAILURE`, making it impossible to distinguish a read-side cache miss from a write-side purge failure.

### Changed
- Cache purge in `updateDocument` is now performed **outside** the database transaction. Previously a cache failure inside the transaction would roll back the committed write; now the DB write is always committed first and the cache is invalidated afterward (half-open / fail-open pattern).
- Cache purge in `deleteDocument` follows the same transactional ordering fix: the row is deleted inside the transaction and the cache entry is evicted only after the transaction commits.
- All event-listener invocations for cache-related events (`EVENT_CACHE_PURGE_FAILURE`, `EVENT_CACHE_READ_FAILURE`, `EVENT_DOCUMENT_PURGE`) are now wrapped in an inner `try/catch`. A listener that throws no longer propagates the exception up to the caller — the error is logged via `Console::error` and execution continues.

### Fixed
- `getDocument` no longer emits `EVENT_CACHE_PURGE_FAILURE` when the cache is unavailable for a read. It now correctly emits `EVENT_CACHE_READ_FAILURE`, so callers that distinguish the two events receive the right signal.
- A broken or unavailable cache can no longer cause `updateDocument` or `deleteDocument` to surface an exception to the caller. Both operations are now fully fail-open with respect to the cache layer.
- A throwing `EVENT_CACHE_PURGE_FAILURE` or `EVENT_CACHE_READ_FAILURE` listener can no longer abort an in-progress database operation.

[Unreleased]: https://github.com/utopia-php/database/compare/6.0.0...HEAD
[6.0.0]: https://github.com/utopia-php/database/compare/5.3.17...6.0.0
