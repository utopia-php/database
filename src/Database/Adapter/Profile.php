<?php

namespace Utopia\Database\Adapter;

use Closure;
use Utopia\Database\Capability;

/**
 * What a database's adapter offers and the mode the database runs in, read once so validators are
 * configured from one value: its limits, the capabilities it supports, the optional features it
 * implements, and whether tables are shared and a migration is running.
 *
 * DefinedAttributes follows the schema mode of the connection a statement runs on, which a pool
 * that has not set the mode leaves to each connection. A profile given $definedAttributes asks it
 * on every check, so validators answer as the database does; DefinedAttributes in $capabilities is
 * then ignored.
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
     * @param  (Closure(): bool)|null  $definedAttributes
     */
    public function __construct(
        public Limits $limits,
        public array $capabilities,
        public array $features,
        public bool $sharedTables,
        public bool $migrating,
        private ?Closure $definedAttributes = null,
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
        if ($capability === Capability::DefinedAttributes && $this->definedAttributes !== null) {
            return ($this->definedAttributes)();
        }

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
