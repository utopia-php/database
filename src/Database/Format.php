<?php

namespace Utopia\Database;

use Utopia\Database\Exception\Structure;

final readonly class Format
{
    /**
     * @param  array<string, mixed>  $options
     *
     * @throws Structure
     */
    public function __construct(
        public string $name,
        public array $options = [],
    ) {
        if ($name === '') {
            throw new Structure('Format name must not be empty');
        }
    }
}
