<?php

namespace Utopia\Database\Profiler;

class QueryProfiler
{
    /** @var array<QueryLog> */
    private array $logs = [];

    private float $slowThreshold = 100.0;

    private bool $enabled = false;

    private bool $captureBacktrace = false;

    /** @var callable|null */
    private $onSlowQuery = null;

    public function enable(): void
    {
        $this->enabled = true;
    }

    public function disable(): void
    {
        $this->enabled = false;
    }

    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    public function setSlowThreshold(float $milliseconds): void
    {
        $this->slowThreshold = $milliseconds;
    }

    public function enableBacktrace(bool $enabled = true): void
    {
        $this->captureBacktrace = $enabled;
    }

    public function onSlowQuery(callable $callback): void
    {
        $this->onSlowQuery = $callback;
    }

    /**
     * @param  array<mixed>  $bindings
     */
    public function log(string $query, array $bindings, float $durationMs, string $collection = '', string $operation = ''): void
    {
        if (! $this->enabled) {
            return;
        }

        $backtrace = null;
        if ($this->captureBacktrace) {
            $trace = \debug_backtrace(\DEBUG_BACKTRACE_IGNORE_ARGS, 10);
            $backtrace = \array_map(
                fn (array $frame) => ($frame['file'] ?? '') . ':' . ($frame['line'] ?? '') . ' ' . ($frame['function'] ?? ''),
                $trace
            );
        }

        $entry = new QueryLog(
            query: $query,
            bindings: $bindings,
            durationMs: $durationMs,
            collection: $collection,
            operation: $operation,
            backtrace: $backtrace,
        );

        $this->logs[] = $entry;

        if ($durationMs >= $this->slowThreshold && $this->onSlowQuery !== null) {
            ($this->onSlowQuery)($entry);
        }
    }

    /**
     * @return array<QueryLog>
     */
    public function getLogs(): array
    {
        return $this->logs;
    }

    /**
     * @return array<QueryLog>
     */
    public function getSlowQueries(): array
    {
        return \array_filter($this->logs, fn (QueryLog $log) => $log->durationMs >= $this->slowThreshold);
    }

    public function getQueryCount(): int
    {
        return \count($this->logs);
    }

    public function getTotalTime(): float
    {
        return \array_sum(\array_map(fn (QueryLog $log) => $log->durationMs, $this->logs));
    }

    /**
     * @return array<string, int>
     */
    public function detectNPlusOne(int $threshold = 5): array
    {
        $patterns = [];

        foreach ($this->logs as $log) {
            $pattern = \preg_replace('/\?(?:,\s*\?)*/', '?...', $log->query) ?? $log->query;
            $pattern = \preg_replace('/\'[^\']*\'/', '?', $pattern) ?? $pattern;
            $pattern = \preg_replace('/\d+/', '?', $pattern) ?? $pattern;

            if (! isset($patterns[$pattern])) {
                $patterns[$pattern] = 0;
            }

            $patterns[$pattern]++;
        }

        return \array_filter($patterns, fn (int $count) => $count >= $threshold);
    }

    public function reset(): void
    {
        $this->logs = [];
    }
}
