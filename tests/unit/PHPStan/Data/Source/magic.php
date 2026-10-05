<?php

namespace Tests\Unit\PHPStan\Data\Source;

use Utopia\Database\Attribute;
use Utopia\Database\Attribute\Integer;
use Utopia\Database\Collection;
use Utopia\Database\Index;
use Utopia\Database\PDOStatement;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\Order;

function magicRead(Attribute $attribute): string
{
    return $attribute->key;
}

function magicWrite(Index $index): void
{
    $index->ttl = 10;
}

function magicIsset(Relationship $relationship): bool
{
    return isset($relationship->twoWay);
}

function magicEmpty(Collection $collection): bool
{
    return empty($collection->attributes);
}

function magicCoalesce(Attribute $attribute): mixed
{
    return $attribute->unknown ?? null;
}

function subclassReceiver(Integer $attribute): int
{
    return $attribute->size;
}

function unionReceiver(Index|\stdClass $model): mixed
{
    return $model->type;
}

function narrowedUnion(Index|\stdClass $model): mixed
{
    if ($model instanceof \stdClass) {
        return $model->type;
    }

    return null;
}

/**
 * @return array<string, mixed>
 */
function nativeMetadata(Collection $collection): array
{
    return $collection->metadata;
}

function getterCall(Attribute $attribute): string
{
    return $attribute->getKey();
}

function nullsafeFetch(?Relationship $relationship): mixed
{
    return $relationship?->side;
}

function nativeStatement(\PDOStatement $statement): string
{
    return $statement->queryString;
}

function wrappedStatement(PDOStatement $statement): mixed
{
    return $statement->queryString;
}

function dynamicName(Attribute $attribute, string $name): mixed
{
    return $attribute->{$name};
}

function collectionName(Collection $collection): string
{
    return $collection->name;
}

function collectionPermissions(Collection $collection): mixed
{
    return $collection->permissions;
}

function collectionDocumentSecurity(Collection $collection): bool
{
    return $collection->documentSecurity;
}

function attributeWrites(Attribute $attribute): void
{
    $attribute->key = 'title';
    $attribute->format = '';
    $attribute->filters = ['json'];
    $attribute->size = 64;
}

function indexWrites(Index $index): void
{
    $index->key = 'by_title';
    $index->type = IndexType::Unique;
    $index->lengths = [32];
    $index->orders = [Order::Asc];
    $index->attributes = ['title'];
}

function relationshipWrites(Relationship $relationship): void
{
    $relationship->type = RelationType::ManyToMany;
    $relationship->key = 'author';
    $relationship->onDelete = ForeignKeyAction::Cascade;
    $relationship->side = RelationSide::Child;
    $relationship->twoWay = true;
}

function collectionWrites(Collection $collection): void
{
    $collection->id = 'books';
    $collection->permissions = null;
    $collection->name = 'Books';
}

function wrappedStatementWrite(PDOStatement $statement): void
{
    $statement->custom = 1;
}

function constantDynamicName(Attribute $attribute): mixed
{
    return $attribute->{'key'};
}

function constantUnionDynamicName(Relationship $relationship, bool $parent): mixed
{
    return $relationship->{$parent ? 'key' : 'twoWayKey'};
}

function constantDynamicWrite(Index $index): void
{
    $index->{'orders'} = [];
}
