<?php

namespace Utopia\Database;

use Closure;
use InvalidArgumentException;
use Utopia\Database\Profiler\Log;

class Profiler
{
    public const int DEFAULT_CAPACITY = 1000;

    /** @var array<int, Log> */
    private array $logs = [];

    private int $oldest = 0;

    private int $capacity = self::DEFAULT_CAPACITY;

    private int $queryCount = 0;

    private float $totalTime = 0.0;

    private float $slowThreshold = 100.0;

    private bool $enabled = false;

    private bool $captureBacktrace = false;

    /** @var (Closure(Log): mixed)|null */
    private ?Closure $onSlowQuery = null;

    public function enable(): static
    {
        $this->enabled = true;

        return $this;
    }

    public function disable(): static
    {
        $this->enabled = false;

        return $this;
    }

    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    public function setSlowThreshold(float $milliseconds): static
    {
        $this->slowThreshold = $milliseconds;

        return $this;
    }

    /**
     * Keep at most this many of the newest entries.
     *
     * @throws InvalidArgumentException
     */
    public function setCapacity(int $capacity): static
    {
        if ($capacity < 1) {
            throw new InvalidArgumentException('Profiler capacity must be at least 1');
        }

        $this->logs = \array_slice($this->getLogs(), -$capacity);
        $this->oldest = 0;
        $this->capacity = $capacity;

        return $this;
    }

    public function getCapacity(): int
    {
        return $this->capacity;
    }

    public function enableBacktrace(bool $enabled = true): static
    {
        $this->captureBacktrace = $enabled;

        return $this;
    }

    public function onSlowQuery(callable $callback): static
    {
        $this->onSlowQuery = $callback(...);

        return $this;
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
                fn (array $frame) => ($frame['file'] ?? '') . ':' . ($frame['line'] ?? '') . ' ' . $frame['function'],
                $trace
            );
        }

        $entry = new Log(
            query: $query,
            bindings: $bindings,
            durationMs: $durationMs,
            collection: $collection,
            operation: $operation,
            backtrace: $backtrace,
        );

        $this->record($entry);

        if ($durationMs >= $this->slowThreshold && $this->onSlowQuery !== null) {
            ($this->onSlowQuery)($entry);
        }
    }

    /**
     * The newest entries up to the capacity, oldest first.
     *
     * @return list<Log>
     */
    public function getLogs(): array
    {
        return [
            ...\array_slice($this->logs, $this->oldest),
            ...\array_slice($this->logs, 0, $this->oldest),
        ];
    }

    /**
     * @return array<Log>
     */
    public function getSlowQueries(): array
    {
        return \array_filter($this->getLogs(), fn (Log $log) => $log->durationMs >= $this->slowThreshold);
    }

    /**
     * Every query logged since the last reset, including those the capacity
     * has since dropped.
     */
    public function getQueryCount(): int
    {
        return $this->queryCount;
    }

    /**
     * The time of every query logged since the last reset, including those
     * the capacity has since dropped.
     */
    public function getTotalTime(): float
    {
        return $this->totalTime;
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
        $this->oldest = 0;
        $this->queryCount = 0;
        $this->totalTime = 0.0;
    }

    private function record(Log $entry): void
    {
        $this->queryCount++;
        $this->totalTime += $entry->durationMs;

        if (\count($this->logs) < $this->capacity) {
            $this->logs[] = $entry;

            return;
        }

        $this->logs[$this->oldest] = $entry;
        $this->oldest = ($this->oldest + 1) % $this->capacity;
    }
}
