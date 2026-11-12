<?php

namespace Tests\Unit\Indexes;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Index;
use Utopia\Query\OrderDirection;

final class LegacyDroppedAttributeIndexTest extends TestCase
{
    public function testAFulltextIndexWhoseOnlyAttributeWasDeletedIsReadWithNoAttributes(): void
    {
        $index = Index::fromDocument(new Document(['$id' => 'category', 'key' => 'category', 'type' => 'fulltext', 'attributes' => [null], 'orders' => ['ASC'], 'lengths' => [200]]));

        $this->assertSame('category', $index->key);
        $this->assertSame([], $index->attributes);
        $this->assertSame([], $index->lengths);
        $this->assertSame([], $index->orders);
    }

    public function testAUniqueIndexKeepsTheAttributesItStillCoversWithTheirLengthsAndOrders(): void
    {
        $index = Index::fromDocument(new Document(['$id' => 'index_2', 'key' => 'index_2', 'type' => 'unique', 'attributes' => ['playId', null], 'orders' => ['ASC', 'ASC'], 'lengths' => [null, null]]));

        $this->assertSame(['playId'], $index->attributes);
        $this->assertSame([null], $index->lengths);
        $this->assertSame([OrderDirection::Asc], $index->orders);
    }

    public function testADroppedAttributeBetweenOthersKeepsEachLengthAndOrderAtItsAttribute(): void
    {
        $index = Index::fromDocument(new Document(['$id' => 'mixed', 'key' => 'mixed', 'type' => 'key', 'attributes' => ['a', null, 'c'], 'orders' => ['ASC', 'DESC', 'DESC'], 'lengths' => [10, 20, 30]]));

        $this->assertSame(['a', 'c'], $index->attributes);
        $this->assertSame([10, 30], $index->lengths);
        $this->assertSame([OrderDirection::Asc, OrderDirection::Desc], $index->orders);
    }

    public function testACollectionHoldingSuchAnIndexIsRead(): void
    {
        $collection = Collection::fromDocument(new Document([
            '$id' => 'plays',
            'name' => 'plays',
            'attributes' => [Attribute::string(key: 'playId', size: 36)->toDocument()],
            'indexes' => [new Document(['$id' => 'index_2', 'key' => 'index_2', 'type' => 'unique', 'attributes' => ['playId', null], 'orders' => ['ASC', 'ASC'], 'lengths' => [null, null]])],
        ]));

        $this->assertSame(['playId'], $collection->indexes()[0]->attributes);
    }

    public function testAnIndexDefinedByACallerStillRefusesAnAttributeThatIsNotAString(): void
    {
        $this->expectException(IndexException::class);
        $this->expectExceptionMessage('Index attributes must be strings');

        Index::fromArray(['$id' => 'bad', 'type' => 'key', 'attributes' => ['playId', null]]);
    }
}
