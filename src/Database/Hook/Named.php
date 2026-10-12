<?php

namespace Utopia\Database\Hook;

/**
 * Gives a {@see Lifecycle} hook a stable name.
 *
 * Registering a named hook replaces the hook already registered under that name, in its
 * position, so re-registration is idempotent; {@see \Utopia\Database\Database::silent()}
 * can silence named hooks individually.
 */
interface Named
{
    public function getName(): string;
}
