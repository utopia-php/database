<?php

declare(strict_types=1);

namespace Utopia\Database\Adapter\Redis;

use Utopia\Database\Document;

final readonly class Write
{
    /**
     * @param  string|null  $payload  The stored payload the write replaces, or null when it creates the document
     */
    public function __construct(
        public string $id,
        public string $key,
        public ?string $payload,
        public Document $document,
        public int|string|null $tenant = null,
    ) {
    }
}
