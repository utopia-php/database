<?php

namespace Utopia\Database\Hook;

use Closure;
use Utopia\Database\Document;

/**
 * How Relationships::prepareNested() prepares the related documents of a write.
 *
 * @internal
 */
final readonly class Preparation
{
    /**
     * @param  list<Nested>  $hooks  The hooks each related document is given to, in order, before it is checked
     * @param  bool  $check  Whether a related document may only add permissions for roles the caller holds
     * @param  bool  $lock  Whether stored permissions are read locked, holding them until the write's transaction ends
     * @param  (Closure(string, string): ?Document)|null  $resolve  The staged view of a related document by collection and ID; null reads the stored one
     */
    public function __construct(
        public array $hooks,
        public bool $check,
        public bool $lock = false,
        public ?Closure $resolve = null,
    ) {
    }
}
