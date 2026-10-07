<?php

namespace Utopia\Database\Profiler;

readonly class Log
{
    /**
     * @param  array<mixed>  $bindings
     * @param  array<string>|null  $backtrace
     */
    public function __construct(
        public string $query,
        public array $bindings,
        public float $durationMs,
        public string $collection = '',
        public string $operation = '',
        public ?array $backtrace = null,
    ) {
    }
}
