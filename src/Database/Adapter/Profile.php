<?php

namespace Utopia\Database\Adapter;

use Utopia\Database\Capability;

/**
 * What a database's adapter offers and the mode the database runs in, read once so validators are
 * configured from one value: its limits, the capabilities it supports, the optional features it
 * implements, and whether tables are shared and a migration is running.
 */
final readonly class Profile
{
    /**
     * @var array<string, true>
     */
    private array $supported;

    /**
     * @var array<string, true>
     */
    private array $implemented;

    /**
     * @param  list<Capability>  $capabilities
     * @param  list<class-string>  $features
     */
    public function __construct(
        public Limits $limits,
        public array $capabilities,
        public array $features,
        public bool $sharedTables,
        public bool $migrating,
    ) {
        $supported = [];
        foreach ($capabilities as $capability) {
            $supported[$capability->name] = true;
        }
        $this->supported = $supported;
        $this->implemented = \array_fill_keys($features, true);
    }

    public function supports(Capability $capability): bool
    {
        return isset($this->supported[$capability->name]);
    }

    /**
     * @param  class-string  $feature
     */
    public function hasFeature(string $feature): bool
    {
        return isset($this->implemented[$feature]);
    }
}
