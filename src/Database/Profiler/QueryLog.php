<?php

namespace Utopia\Database\Profiler;

readonly class QueryLog
{
    /**
     * @param  array<mixed>  $bindings
     * @param  array<string>|null  $backtrace
     */
    public function __construct(
        public string $query,
        public array $bindings,
        public float $durationMs,
        public ?string $explainPlan = null,
        public string $collection = '',
        public string $operation = '',
        public ?array $backtrace = null,
    ) {
    }
}
