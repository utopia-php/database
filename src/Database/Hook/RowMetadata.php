<?php

namespace Utopia\Database\Hook;

/**
 * What a write hook knows about the document a row is written for.
 */
final readonly class RowMetadata
{
    /**
     * @param  int|string|null  $tenant  The document's tenant, or the adapter's when the document names none
     */
    public function __construct(
        public int|string|null $tenant,
    ) {
    }
}
