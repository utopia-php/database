<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Attribute;

/**
 * One relationship a cascading delete is following: the relationship attribute, the collection declaring it and the
 * document whose delete reached it.
 *
 * @internal
 */
final readonly class Cascade
{
    public function __construct(
        public string $collection,
        public string $document,
        public Attribute $attribute,
    ) {
    }
}
