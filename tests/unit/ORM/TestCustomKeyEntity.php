<?php

namespace Tests\Unit\ORM;

use Utopia\Database\ORM\Mapping\Column;
use Utopia\Database\ORM\Mapping\Entity;
use Utopia\Database\ORM\Mapping\Id;
use Utopia\Query\Schema\ColumnType;

#[Entity(collection: 'custom_keys')]
class TestCustomKeyEntity
{
    #[Id]
    public string $id = '';

    #[Column(type: ColumnType::String, size: 100, key: 'display_name')]
    public string $displayName = '';
}
