<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Index;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\Order;

final class IndexTest extends TestCase
{
    public function testConstructorIsPrivate(): void
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Call to private Utopia\Database\Index::__construct()');

        /** @phpstan-ignore new.privateConstructor */
        $index = new Index('by_age', IndexType::Key, ['age'], [], [], null);
    }

    public function testPropertiesAreReadonly(): void
    {
        $index = Index::key('by_age', ['age']);

        $this->expectException(\Error::class);

        /** @phpstan-ignore assign.propertyProtectedSet */
        $index->key = 'renamed';
    }

    public function testFromDocumentReadsTheStoredShape(): void
    {
        $index = Index::fromDocument(new Document([
            '$id' => 'by_name',
            'key' => 'by_name',
            'type' => 'unique',
            'attributes' => ['first', 'last'],
            'lengths' => [64, null],
            'orders' => ['ASC', null],
        ]));

        $this->assertSame('by_name', $index->key);
        $this->assertSame(IndexType::Unique, $index->type);
        $this->assertSame(['first', 'last'], $index->attributes);
        $this->assertSame([64, null], $index->lengths);
        $this->assertSame([OrderDirection::Asc, null], $index->orders);
        $this->assertNull($index->ttl);
    }

    public function testFromDocumentReadsTheLegacyIndexTypeAsKey(): void
    {
        $index = Index::fromDocument(new Document(['$id' => 'legacy', 'type' => 'index', 'attributes' => ['age']]));

        $this->assertSame(IndexType::Key, $index->type);
        $this->assertSame('key', $index->toDocument()->getAttribute('type'));
    }

    public function testFromDocumentIgnoresTheLegacyTtlOnNonTtlIndexes(): void
    {
        $index = Index::fromDocument(new Document([
            '$id' => 'by_age',
            'type' => 'key',
            'attributes' => ['age'],
            'ttl' => 1,
        ]));

        $this->assertNull($index->ttl);
        $this->assertFalse($index->toDocument()->isSet('ttl'));
    }

    public function testFromDocumentToleratesLowercaseAndEmptyOrders(): void
    {
        $index = Index::fromDocument(new Document([
            '$id' => 'by_name',
            'type' => 'key',
            'attributes' => ['first', 'middle', 'last'],
            'orders' => ['asc', 'desc', ''],
        ]));

        $this->assertSame([OrderDirection::Asc, OrderDirection::Desc, null], $index->orders);
    }

    public function testFromArrayToleratesSchemaOrderCases(): void
    {
        $index = Index::fromArray(['key' => 'by_age', 'type' => 'key', 'attributes' => ['age'], 'orders' => [Order::Desc]]);

        $this->assertSame([OrderDirection::Desc], $index->orders);
    }

    public function testFromDocumentReadsNumericStringLengthsAndTtl(): void
    {
        $key = Index::fromDocument(new Document(['$id' => 'by_name', 'type' => 'key', 'attributes' => ['name'], 'lengths' => ['32']]));
        $ttl = Index::fromDocument(new Document(['$id' => 'expiry', 'type' => 'ttl', 'attributes' => ['expiresAt'], 'ttl' => '60']));

        $this->assertSame([32], $key->lengths);
        $this->assertSame(60, $ttl->ttl);
    }

    public function testFromDocumentFallsBackToTheIdForTheKey(): void
    {
        $index = Index::fromDocument(new Document(['$id' => 'by_age', 'type' => 'key', 'attributes' => ['age']]));

        $this->assertSame('by_age', $index->key);
    }

    public function testFromDocumentDefaultsAMissingTypeToKey(): void
    {
        $index = Index::fromDocument(new Document(['$id' => 'by_age', 'attributes' => ['age']]));

        $this->assertSame(IndexType::Key, $index->type);
    }

    public function testFromDocumentRejectsAnUnknownType(): void
    {
        $this->expectException(IndexException::class);

        Index::fromDocument(new Document(['$id' => 'odd', 'type' => 'bitmap', 'attributes' => ['age']]));
    }

    public function testFromDocumentRejectsAStoredRandomOrder(): void
    {
        $this->expectException(IndexException::class);
        $this->expectExceptionMessage('Index orders cannot be random');

        Index::fromDocument(new Document(['$id' => 'by_age', 'type' => 'key', 'attributes' => ['age'], 'orders' => ['RANDOM']]));
    }

    public function testFromDocumentRejectsAnUnknownOrder(): void
    {
        $this->expectException(IndexException::class);

        Index::fromDocument(new Document(['$id' => 'by_age', 'type' => 'key', 'attributes' => ['age'], 'orders' => ['sideways']]));
    }

    public function testFromDocumentRejectsATtlIndexWithoutATtl(): void
    {
        $this->expectException(IndexException::class);
        $this->expectExceptionMessage('TTL must be at least 1 second');

        Index::fromDocument(new Document(['$id' => 'expiry', 'type' => 'ttl', 'attributes' => ['expiresAt']]));
    }

    public function testFromArrayPrefersTheKeyOverTheId(): void
    {
        $index = Index::fromArray(['$id' => 'old', 'key' => 'new', 'type' => 'key', 'attributes' => ['age']]);

        $this->assertSame('new', $index->key);
    }

    public function testFromArrayAcceptsTypedValues(): void
    {
        $index = Index::fromArray([
            'key' => 'by_age',
            'type' => IndexType::Unique,
            'attributes' => ['age'],
            'orders' => [OrderDirection::Desc],
        ]);

        $this->assertSame(IndexType::Unique, $index->type);
        $this->assertSame([OrderDirection::Desc], $index->orders);
    }

    public function testToDocumentRoundTripsThroughFromDocument(): void
    {
        $index = Index::key('by_name', ['first', 'last'], [32, null], [OrderDirection::Desc, null]);
        $document = $index->toDocument();

        $read = Index::fromDocument($document);

        $this->assertSame($index->key, $read->key);
        $this->assertSame($index->type, $read->type);
        $this->assertSame($index->attributes, $read->attributes);
        $this->assertSame($index->lengths, $read->lengths);
        $this->assertSame($index->orders, $read->orders);
        $this->assertSame($index->ttl, $read->ttl);
        $this->assertSame($document->getArrayCopy(), $read->toDocument()->getArrayCopy());
    }

    public function testWithKeyReturnsARenamedCopy(): void
    {
        $index = Index::key('by_age', ['age'], [8], [OrderDirection::Asc]);
        $renamed = $index->withKey('by_years');

        $this->assertSame('by_years', $renamed->key);
        $this->assertSame('by_years', $renamed->toDocument()->getId());
        $this->assertSame(['age'], $renamed->attributes);
        $this->assertSame([8], $renamed->lengths);
        $this->assertSame([OrderDirection::Asc], $renamed->orders);
        $this->assertSame('by_age', $index->key);
    }

    public function testWithLengthsReturnsACopyWithNewLengths(): void
    {
        $index = Index::key('by_name', ['first', 'last'], [16, 16]);
        $changed = $index->withLengths([null, 64]);

        $this->assertSame([null, 64], $changed->lengths);
        $this->assertSame([16, 16], $index->lengths);
    }

    public function testWithLengthsRejectsANonIntegerLength(): void
    {
        $this->expectException(IndexException::class);

        /** @phpstan-ignore argument.type */
        Index::key('by_name', ['name'])->withLengths(['wide']);
    }

    public function testWithOrdersReturnsACopyWithNewOrders(): void
    {
        $index = Index::key('by_name', ['first', 'last'], [], [OrderDirection::Asc, OrderDirection::Asc]);
        $changed = $index->withOrders([null, OrderDirection::Desc]);

        $this->assertSame([null, OrderDirection::Desc], $changed->orders);
        $this->assertSame([OrderDirection::Asc, OrderDirection::Asc], $index->orders);
    }

    public function testWithOrdersRejectsARandomOrder(): void
    {
        $this->expectException(IndexException::class);
        $this->expectExceptionMessage('Index orders cannot be random');

        Index::key('by_age', ['age'])->withOrders([OrderDirection::Random]);
    }

    /**
     * @return array<string, array{Index}>
     */
    public static function orderlessIndexes(): array
    {
        return [
            'fulltext' => [Index::fulltext('search', ['title'])],
            'ttl' => [Index::ttl('expiry', 'expiresAt', 60)],
        ];
    }

    #[DataProvider('orderlessIndexes')]
    public function testWithOrdersRejectsOrdersTheIndexCannotStore(Index $index): void
    {
        $this->expectException(IndexException::class);

        $index->withOrders([OrderDirection::Asc]);
    }

    #[DataProvider('orderlessIndexes')]
    public function testWithOrdersAcceptsNoOrdersOnAnOrderlessIndex(Index $index): void
    {
        $this->assertSame([], $index->withOrders([])->orders);
    }

    public function testWithLengthsRejectsLengthsOnAFulltextIndex(): void
    {
        $this->expectException(IndexException::class);

        Index::fulltext('search', ['title'])->withLengths([16]);
    }

    public function testWithLengthsAcceptsNoLengthsOnAFulltextIndex(): void
    {
        $this->assertSame([], Index::fulltext('search', ['title'])->withLengths([])->lengths);
    }

    public function testWithLengthsOnATtlIndexArePersisted(): void
    {
        $index = Index::ttl('expiry', 'expiresAt', 60)->withLengths([8]);

        $this->assertSame([8], $index->toDocument()->getAttribute('lengths'));
    }
}
