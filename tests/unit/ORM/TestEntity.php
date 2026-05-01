<?php

namespace Tests\Unit\ORM;

use Utopia\Database\ORM\Mapping\Column;
use Utopia\Database\ORM\Mapping\CreatedAt;
use Utopia\Database\ORM\Mapping\Entity;
use Utopia\Database\ORM\Mapping\HasMany;
use Utopia\Database\ORM\Mapping\Id;
use Utopia\Database\ORM\Mapping\Permissions;
use Utopia\Database\ORM\Mapping\TableIndex;
use Utopia\Database\ORM\Mapping\UpdatedAt;
use Utopia\Database\ORM\Mapping\Version;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

#[Entity(collection: 'users', documentSecurity: true)]
#[TableIndex(key: 'idx_email', type: IndexType::Unique, attributes: ['email'])]
#[TableIndex(key: 'idx_name', type: IndexType::Index, attributes: ['name'])]
class TestEntity
{
    #[Id]
    public string $id = '';

    #[Version]
    public ?int $version = null;

    #[CreatedAt]
    public ?string $createdAt = null;

    #[UpdatedAt]
    public ?string $updatedAt = null;

    #[Permissions]
    public array $permissions = [];

    #[Column(type: ColumnType::String, size: 255, required: true)]
    public string $name = '';

    #[Column(type: ColumnType::String, size: 255, required: true)]
    public string $email = '';

    #[Column(type: ColumnType::Integer, size: 0)]
    public int $age = 0;

    #[Column(type: ColumnType::Boolean)]
    public bool $active = true;

    #[HasMany(target: TestPost::class, key: 'posts', twoWayKey: 'author')]
    public array $posts = [];
}
