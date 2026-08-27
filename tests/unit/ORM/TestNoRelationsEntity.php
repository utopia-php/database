<?php

namespace Tests\Unit\ORM;

use Utopia\Database\ORM\Mapping\Column;
use Utopia\Database\ORM\Mapping\Entity;
use Utopia\Database\ORM\Mapping\Id;
use Utopia\Query\Schema\ColumnType;

#[Entity(collection: 'no_relations')]
class TestNoRelationsEntity
{
    #[Id]
    public string $id = '';

    #[Column(type: ColumnType::String, size: 100)]
    public string $label = '';
}
