<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Index;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\IndexType;

final class IndexFactoryTest extends TestCase
{
    /**
     * @return iterable<string, array{Index, array<string, mixed>}>
     */
    public static function persistedShapes(): iterable
    {
        yield 'key' => [
            Index::key('by_name', ['first', 'last'], [64, null], [OrderDirection::Asc, OrderDirection::Desc]),
            ['$id' => 'by_name', 'key' => 'by_name', 'type' => 'key', 'attributes' => ['first', 'last'], 'lengths' => [64, null], 'orders' => ['ASC', 'DESC']],
        ];
        yield 'key without lengths or orders' => [
            Index::key('by_age', ['age']),
            ['$id' => 'by_age', 'key' => 'by_age', 'type' => 'key', 'attributes' => ['age'], 'lengths' => [], 'orders' => []],
        ];
        yield 'unique' => [
            Index::unique('by_email', ['email'], [128], [null]),
            ['$id' => 'by_email', 'key' => 'by_email', 'type' => 'unique', 'attributes' => ['email'], 'lengths' => [128], 'orders' => [null]],
        ];
        yield 'fulltext stores no lengths or orders' => [
            Index::fulltext('search', ['title', 'body']),
            ['$id' => 'search', 'key' => 'search', 'type' => 'fulltext', 'attributes' => ['title', 'body']],
        ];
        yield 'trigram' => [
            Index::trigram('fuzzy', ['name']),
            ['$id' => 'fuzzy', 'key' => 'fuzzy', 'type' => 'trigram', 'attributes' => ['name'], 'lengths' => [], 'orders' => []],
        ];
        yield 'spatial' => [
            Index::spatial('where', 'location'),
            ['$id' => 'where', 'key' => 'where', 'type' => 'spatial', 'attributes' => ['location'], 'lengths' => [], 'orders' => []],
        ];
        yield 'spatial with an order' => [
            Index::spatial('where', 'location', OrderDirection::Desc),
            ['$id' => 'where', 'key' => 'where', 'type' => 'spatial', 'attributes' => ['location'], 'lengths' => [], 'orders' => ['DESC']],
        ];
        yield 'object' => [
            Index::object('payload', 'data'),
            ['$id' => 'payload', 'key' => 'payload', 'type' => 'object', 'attributes' => ['data'], 'lengths' => [], 'orders' => []],
        ];
        yield 'hnsw euclidean' => [
            Index::hnswEuclidean('near', 'embedding'),
            ['$id' => 'near', 'key' => 'near', 'type' => 'hnsw_euclidean', 'attributes' => ['embedding'], 'lengths' => [], 'orders' => []],
        ];
        yield 'hnsw cosine' => [
            Index::hnswCosine('near', 'embedding'),
            ['$id' => 'near', 'key' => 'near', 'type' => 'hnsw_cosine', 'attributes' => ['embedding'], 'lengths' => [], 'orders' => []],
        ];
        yield 'hnsw dot' => [
            Index::hnswDot('near', 'embedding'),
            ['$id' => 'near', 'key' => 'near', 'type' => 'hnsw_dot', 'attributes' => ['embedding'], 'lengths' => [], 'orders' => []],
        ];
        yield 'ttl stores ttl and no orders' => [
            Index::ttl('expiry', 'expiresAt', 3600),
            ['$id' => 'expiry', 'key' => 'expiry', 'type' => 'ttl', 'attributes' => ['expiresAt'], 'lengths' => [], 'ttl' => 3600],
        ];
    }

    /**
     * @param  array<string, mixed>  $expected
     */
    #[DataProvider('persistedShapes')]
    public function testFactoryPersistsItsShape(Index $index, array $expected): void
    {
        $this->assertSame($expected, $index->toDocument()->getArrayCopy());
    }

    /**
     * @param  array<string, mixed>  $expected
     */
    #[DataProvider('persistedShapes')]
    public function testPersistedShapeRoundTrips(Index $index, array $expected): void
    {
        $this->assertSame($expected, Index::fromDocument($index->toDocument())->toDocument()->getArrayCopy());
        $this->assertSame($expected, Index::fromArray($expected)->toDocument()->getArrayCopy());
    }

    public function testOnlyTtlIndexesCarryATtl(): void
    {
        $this->assertSame(3600, Index::ttl('expiry', 'expiresAt', 3600)->ttl);
        $this->assertNull(Index::key('by_age', ['age'])->ttl);
        $this->assertNull(Index::fulltext('search', ['title'])->ttl);
    }

    public function testFactoriesExposeTypedProperties(): void
    {
        $index = Index::unique('by_name', ['first', 'last'], [32, null], [OrderDirection::Asc, null]);

        $this->assertSame('by_name', $index->key);
        $this->assertSame(IndexType::Unique, $index->type);
        $this->assertSame(['first', 'last'], $index->attributes);
        $this->assertSame([32, null], $index->lengths);
        $this->assertSame([OrderDirection::Asc, null], $index->orders);
    }

    /**
     * @return iterable<string, array{\Closure(): Index}>
     */
    public static function randomOrders(): iterable
    {
        yield 'key' => [static fn (): Index => Index::key('by_age', ['age'], [], [OrderDirection::Random])];
        yield 'unique' => [static fn (): Index => Index::unique('by_age', ['age'], [], [OrderDirection::Asc, OrderDirection::Random])];
        yield 'spatial' => [static fn (): Index => Index::spatial('where', 'location', OrderDirection::Random)];
    }

    /**
     * @param  \Closure(): Index  $factory
     */
    #[DataProvider('randomOrders')]
    public function testRandomOrderIsRejected(\Closure $factory): void
    {
        $this->expectException(IndexException::class);
        $this->expectExceptionMessage('Index orders cannot be random');

        $factory();
    }

    /**
     * @return iterable<string, array{int}>
     */
    public static function invalidTtls(): iterable
    {
        yield 'zero' => [0];
        yield 'negative' => [-1];
    }

    #[DataProvider('invalidTtls')]
    public function testTtlBelowOneIsRejected(int $ttl): void
    {
        $this->expectException(IndexException::class);
        $this->expectExceptionMessage('TTL must be at least 1 second');

        Index::ttl('expiry', 'expiresAt', $ttl);
    }

    public function testTtlOfOneSecondIsAccepted(): void
    {
        $this->assertSame(1, Index::ttl('expiry', 'expiresAt', 1)->ttl);
    }

    public function testFactoryRejectsAnOrderThatIsNotADirection(): void
    {
        $this->expectException(IndexException::class);

        /** @phpstan-ignore argument.type */
        Index::key('by_age', ['age'], [], ['ASC']);
    }

    public function testFactoryRejectsALengthThatIsNotAnInteger(): void
    {
        $this->expectException(IndexException::class);

        /** @phpstan-ignore argument.type */
        Index::key('by_name', ['name'], ['64']);
    }

    public function testFactoryRejectsAnAttributeThatIsNotAString(): void
    {
        $this->expectException(IndexException::class);

        /** @phpstan-ignore argument.type */
        Index::key('by_name', [1]);
    }

    public function testHydratedListsAreReindexed(): void
    {
        $index = Index::fromArray([
            'key' => 'by_name',
            'type' => 'key',
            'attributes' => [3 => 'first', 7 => 'last'],
            'lengths' => [2 => 16],
            'orders' => [5 => 'desc'],
        ]);

        $this->assertSame(['first', 'last'], $index->attributes);
        $this->assertSame([16], $index->lengths);
        $this->assertSame([OrderDirection::Desc], $index->orders);
    }
}
