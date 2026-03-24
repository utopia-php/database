<?php

namespace Tests\Unit\ORM;

use Utopia\Database\ORM\Mapping\BelongsTo;
use Utopia\Database\ORM\Mapping\BelongsToMany;
use Utopia\Database\ORM\Mapping\Entity;
use Utopia\Database\ORM\Mapping\HasMany;
use Utopia\Database\ORM\Mapping\HasOne;
use Utopia\Database\ORM\Mapping\Id;

#[Entity(collection: 'all_relations')]
class TestAllRelationsEntity
{
    #[Id]
    public string $id = '';

    #[HasOne(target: TestNoRelationsEntity::class, key: 'profile', twoWayKey: 'owner')]
    public mixed $profile = null;

    #[BelongsTo(target: TestNoRelationsEntity::class, key: 'team', twoWayKey: 'members')]
    public mixed $team = null;

    #[HasMany(target: TestPost::class, key: 'posts', twoWayKey: 'author')]
    public array $posts = [];

    #[BelongsToMany(target: TestNoRelationsEntity::class, key: 'tags', twoWayKey: 'items')]
    public array $tags = [];
}
