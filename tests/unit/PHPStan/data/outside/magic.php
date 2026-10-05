<?php

namespace Tests\Unit\PHPStan\Data\Outside;

use Utopia\Database\Attribute;
use Utopia\Database\Attribute\Integer;
use Utopia\Database\Collection;
use Utopia\Database\Index;
use Utopia\Database\PDOStatement;
use Utopia\Database\Relationship;

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
