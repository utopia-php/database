<?php

namespace Tests\Unit\Support;

use PDO;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Capability;

enum Engine: string
{
    case Memory = 'memory';
    case SchemalessMemory = 'schemaless memory';
    case SQLite = 'sqlite';

    public function adapter(): Adapter
    {
        return match ($this) {
            self::Memory => new Memory(),
            self::SchemalessMemory => new class () extends Memory {
                #[\Override]
                public function capabilities(): array
                {
                    return \array_values(\array_filter(
                        parent::capabilities(),
                        static fn (Capability $capability): bool => $capability !== Capability::DefinedAttributes,
                    ));
                }
            },
            self::SQLite => new SQLite(new PDO('sqlite::memory:')),
        };
    }
}
