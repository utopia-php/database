<?php

namespace Tests\Unit\ORM;

use Utopia\Database\ORM\Mapping\BelongsTo;
use Utopia\Database\ORM\Mapping\Column;
use Utopia\Database\ORM\Mapping\Entity;
use Utopia\Database\ORM\Mapping\Id;
use Utopia\Query\Schema\ColumnType;

#[Entity(collection: 'posts')]
class TestPost
{
    #[Id]
    public string $id = '';

    #[Column(type: ColumnType::String, size: 255, required: true)]
    public string $title = '';

    #[Column(type: ColumnType::String, size: 10000)]
    public string $content = '';

    #[BelongsTo(target: TestEntity::class, key: 'author', twoWayKey: 'posts')]
    public mixed $author = null;
}
