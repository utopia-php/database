<?php

namespace Utopia\Database;

/**
 * A document write: the stored document it replaces, empty when it creates one, and the document to write.
 */
final readonly class Change
{
    public function __construct(
        public Document $old,
        public Document $new,
    ) {
    }
}
