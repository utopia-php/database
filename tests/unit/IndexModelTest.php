<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionParameter;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\Order;

class IndexModelTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $index = Index::key(key: 'idx_test');

        $this->assertSame('idx_test', $index->key);
        $this->assertSame(IndexType::Key, $index->type);
        $this->assertSame([], $index->attributes);
        $this->assertSame([], $index->lengths);
        $this->assertSame([], $index->orders);
        $this->assertSame(1, $index->ttl);
    }

    public function testConstructorWithAllValues(): void
    {
        $index = Index::unique(
            key: 'idx_compound',
            attributes: ['name', 'email'],
            lengths: [128, 256],
            orders: [Order::Asc, Order::Desc],
            ttl: 3600,
        );

        $this->assertSame('idx_compound', $index->key);
        $this->assertSame(IndexType::Unique, $index->type);
        $this->assertSame(['name', 'email'], $index->attributes);
        $this->assertSame([128, 256], $index->lengths);
        $this->assertSame([Order::Asc, Order::Desc], $index->orders);
        $this->assertSame(3600, $index->ttl);
    }

    public function testConstructorRejectsStringOrders(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Index order must be Order or null');

        // @phpstan-ignore-next-line
        Index::key(key: 'idx_enabled', attributes: ['enabled'], orders: ['ASC']);
    }

    public function testConstructorAcceptsOrderEnum(): void
    {
        $index = Index::key(
            key: 'idx_enabled',
            attributes: ['enabled'],
            orders: [Order::Asc, Order::Desc],
        );

        $this->assertSame([Order::Asc, Order::Desc], $index->orders);
        $this->assertSame(['ASC', 'DESC'], $index->toDocument()->getAttribute('orders'));
    }

    public function testToDocumentProducesCorrectStructure(): void
    {
        $index = Index::unique(key: 'idx_email', attributes: ['email'], lengths: [256], orders: [Order::Asc]);

        $doc = $index->toDocument();

        $this->assertSame('idx_email', $doc->getId());
        $this->assertSame('idx_email', $doc->getAttribute('key'));
        $this->assertSame('unique', $doc->getAttribute('type'));
        $this->assertSame(['email'], $doc->getArray('attributes'));
        $this->assertSame([256], $doc->getAttribute('lengths'));
        $this->assertSame(['ASC'], $doc->getAttribute('orders'));
        $this->assertSame(1, $doc->getAttribute('ttl'));
    }

    public function testArrayAccessMatchesDocumentShape(): void
    {
        $index = Index::unique(key: 'idx_email', attributes: ['email'], lengths: [128], orders: [Order::Asc]);

        $this->assertSame('idx_email', $index['$id']);
        $this->assertSame('idx_email', $index['key']);
        $this->assertSame(IndexType::Unique->value, $index['type']);
        $this->assertSame(['email'], $index['attributes']);
        $this->assertSame('idx_email', $index->getId());
    }

    public function testFromDocumentRoundtrip(): void
    {
        $original = Index::key(
            key: 'idx_status_name',
            attributes: ['status', 'name'],
            lengths: [32, 128],
            orders: [Order::Asc, Order::Asc],
            ttl: 7200,
        );

        $doc = $original->toDocument();
        $restored = Index::fromDocument($doc);

        $this->assertSame($original->key, $restored->key);
        $this->assertSame($original->type, $restored->type);
        $this->assertSame($original->attributes, $restored->attributes);
        $this->assertSame($original->lengths, $restored->lengths);
        $this->assertSame($original->orders, $restored->orders);
        $this->assertSame($original->ttl, $restored->ttl);
    }

    public function testFromDocumentWithMinimalDocument(): void
    {
        $doc = new Document([
            '$id' => 'idx_min',
            'type' => 'key',
        ]);

        $index = Index::fromDocument($doc);

        $this->assertSame('idx_min', $index->key);
        $this->assertSame(IndexType::Key, $index->type);
        $this->assertSame([], $index->attributes);
        $this->assertSame([], $index->lengths);
        $this->assertSame([], $index->orders);
        $this->assertSame(1, $index->ttl);
    }

    public function testFromDocumentUsesKeyOverId(): void
    {
        $doc = new Document([
            '$id' => 'id_value',
            'key' => 'key_value',
            'type' => 'index',
        ]);

        $index = Index::fromDocument($doc);
        $this->assertSame('key_value', $index->key);
    }

    public function testAllIndexTypeValues(): void
    {
        $types = [
            IndexType::Key,
            IndexType::Index,
            IndexType::Unique,
            IndexType::Fulltext,
            IndexType::Spatial,
            IndexType::HnswEuclidean,
            IndexType::HnswCosine,
            IndexType::HnswDot,
            IndexType::Trigram,
            IndexType::Ttl,
        ];

        foreach ($types as $type) {
            $index = new Index(key: 'idx_' . $type->value, type: $type, attributes: ['col']);
            $doc = $index->toDocument();
            $restored = Index::fromDocument($doc);

            $this->assertSame($type, $restored->type, "Roundtrip failed for type: {$type->value}");
        }
    }

    public function testWithTTL(): void
    {
        $index = Index::ttl(key: 'idx_ttl', attributes: ['expiresAt'], ttl: 86400);

        $doc = $index->toDocument();
        $this->assertSame(86400, $doc->getAttribute('ttl'));

        $restored = Index::fromDocument($doc);
        $this->assertSame(86400, $restored->ttl);
    }

    public function testWithNullLengthsAndOrders(): void
    {
        $index = Index::key(key: 'idx_mixed', attributes: ['a', 'b'], lengths: [128, null], orders: [Order::Asc, null]);

        $doc = $index->toDocument();
        $this->assertSame([128, null], $doc->getAttribute('lengths'));
        $this->assertSame(['ASC', null], $doc->getAttribute('orders'));

        $restored = Index::fromDocument($doc);
        $this->assertSame([128, null], $restored->lengths);
        $this->assertSame([Order::Asc, null], $restored->orders);
    }

    public function testMultipleAttributeIndex(): void
    {
        $index = Index::key(key: 'idx_multi', attributes: ['firstName', 'lastName', 'email'], lengths: [64, 64, 256], orders: [Order::Asc, Order::Asc, Order::Desc]);

        $doc = $index->toDocument();
        $restored = Index::fromDocument($doc);

        $this->assertCount(3, $restored->attributes);
        $this->assertCount(3, $restored->lengths);
        $this->assertCount(3, $restored->orders);
    }

    /**
     * @return array<string, array{string, IndexType}>
     */
    public static function factories(): array
    {
        return [
            'key' => ['key', IndexType::Key],
            'index' => ['index', IndexType::Index],
            'unique' => ['unique', IndexType::Unique],
            'fullText' => ['fullText', IndexType::Fulltext],
            'spatial' => ['spatial', IndexType::Spatial],
            'object' => ['object', IndexType::Object],
            'hnswEuclidean' => ['hnswEuclidean', IndexType::HnswEuclidean],
            'hnswCosine' => ['hnswCosine', IndexType::HnswCosine],
            'hnswDot' => ['hnswDot', IndexType::HnswDot],
            'trigram' => ['trigram', IndexType::Trigram],
            'ttl' => ['ttl', IndexType::Ttl],
        ];
    }

    #[DataProvider('factories')]
    public function testFactorySetsTypeAndDefaults(string $factory, IndexType $type): void
    {
        $index = Index::{$factory}(key: 'idx');

        $this->assertInstanceOf(Index::class, $index);
        $this->assertSame('idx', $index->key);
        $this->assertSame($type, $index->type);
        $this->assertSame([], $index->attributes);
        $this->assertSame([], $index->lengths);
        $this->assertSame([], $index->orders);
        $this->assertSame(1, $index->ttl);
    }

    public function testFactoryOmitsTypeParameter(): void
    {
        $names = array_map(
            static fn (ReflectionParameter $parameter): string => $parameter->getName(),
            (new ReflectionMethod(Index::class, 'key'))->getParameters(),
        );

        $this->assertSame(false, in_array('type', $names, true));
    }

    public function testFactoryForwardsOptionalArguments(): void
    {
        $index = Index::key(
            key: 'idx',
            attributes: ['name', 'email'],
            lengths: [128, 256],
            orders: [Order::Asc, Order::Desc],
            ttl: 3600,
        );

        $this->assertSame('idx', $index->key);
        $this->assertSame(IndexType::Key, $index->type);
        $this->assertSame(['name', 'email'], $index->attributes);
        $this->assertSame([128, 256], $index->lengths);
        $this->assertSame([Order::Asc, Order::Desc], $index->orders);
        $this->assertSame(3600, $index->ttl);
    }

    public function testKeyFactory(): void
    {
        $index = Index::key('idx_name', ['name']);

        $this->assertSame('idx_name', $index->key);
        $this->assertSame(IndexType::Key, $index->type);
        $this->assertSame(['name'], $index->attributes);
    }

    public function testFullTextFactory(): void
    {
        $index = Index::fullText('idx_body', ['body']);

        $this->assertSame('idx_body', $index->key);
        $this->assertSame(IndexType::Fulltext, $index->type);
        $this->assertSame(['body'], $index->attributes);
    }
}
