<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Hook\Transform;
use Utopia\Database\Profiler;

final readonly class SyncSnapshot
{
    /**
     * @param  array<string, mixed>  $metadata
     * @param  array<string, Transform>  $transforms
     * @param  array<string, int>  $timeouts
     */
    public function __construct(
        public string $database,
        public string $namespace,
        public int|string|null $tenant,
        public array $metadata,
        public array $transforms,
        public ?Profiler $profiler,
        public bool $schemaless,
        public array $timeouts,
    ) {
    }
}
