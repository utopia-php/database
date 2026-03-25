<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Query\Schema\IndexType;

class IndexModelTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $index = new Index(key: 'idx_test', type: IndexType::Key);

        $this->assertSame('idx_test', $index->key);
        $this->assertSame(IndexType::Key, $index->type);
        $this->assertSame([], $index->attributes);
        $this->assertSame([], $index->lengths);
        $this->assertSame([], $index->orders);
        $this->assertSame(1, $index->ttl);
    }

    public function testConstructorWithAllValues(): void
    {
        $index = new Index(
            key: 'idx_compound',
            type: IndexType::Unique,
            attributes: ['name', 'email'],
            lengths: [128, 256],
            orders: ['ASC', 'DESC'],
            ttl: 3600,
        );

        $this->assertSame('idx_compound', $index->key);
        $this->assertSame(IndexType::Unique, $index->type);
        $this->assertSame(['name', 'email'], $index->attributes);
        $this->assertSame([128, 256], $index->lengths);
        $this->assertSame(['ASC', 'DESC'], $index->orders);
        $this->assertSame(3600, $index->ttl);
    }

    public function testToDocumentProducesCorrectStructure(): void
    {
        $index = new Index(
            key: 'idx_email',
            type: IndexType::Unique,
            attributes: ['email'],
            lengths: [256],
            orders: ['ASC'],
            ttl: 1,
        );

        $doc = $index->toDocument();

        $this->assertInstanceOf(Document::class, $doc);
        $this->assertSame('idx_email', $doc->getId());
        $this->assertSame('idx_email', $doc->getAttribute('key'));
        $this->assertSame('unique', $doc->getAttribute('type'));
        $this->assertSame(['email'], $doc->getAttribute('attributes'));
        $this->assertSame([256], $doc->getAttribute('lengths'));
        $this->assertSame(['ASC'], $doc->getAttribute('orders'));
        $this->assertSame(1, $doc->getAttribute('ttl'));
    }

    public function testFromDocumentRoundtrip(): void
    {
        $original = new Index(
            key: 'idx_status_name',
            type: IndexType::Key,
            attributes: ['status', 'name'],
            lengths: [32, 128],
            orders: ['ASC', 'ASC'],
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
        $index = new Index(
            key: 'idx_ttl',
            type: IndexType::Ttl,
            attributes: ['expiresAt'],
            ttl: 86400,
        );

        $doc = $index->toDocument();
        $this->assertSame(86400, $doc->getAttribute('ttl'));

        $restored = Index::fromDocument($doc);
        $this->assertSame(86400, $restored->ttl);
    }

    public function testWithNullLengthsAndOrders(): void
    {
        $index = new Index(
            key: 'idx_mixed',
            type: IndexType::Key,
            attributes: ['a', 'b'],
            lengths: [128, null],
            orders: ['ASC', null],
        );

        $doc = $index->toDocument();
        $this->assertSame([128, null], $doc->getAttribute('lengths'));
        $this->assertSame(['ASC', null], $doc->getAttribute('orders'));

        $restored = Index::fromDocument($doc);
        $this->assertSame([128, null], $restored->lengths);
        $this->assertSame(['ASC', null], $restored->orders);
    }

    public function testMultipleAttributeIndex(): void
    {
        $index = new Index(
            key: 'idx_multi',
            type: IndexType::Key,
            attributes: ['firstName', 'lastName', 'email'],
            lengths: [64, 64, 256],
            orders: ['ASC', 'ASC', 'DESC'],
        );

        $doc = $index->toDocument();
        $restored = Index::fromDocument($doc);

        $this->assertCount(3, $restored->attributes);
        $this->assertCount(3, $restored->lengths);
        $this->assertCount(3, $restored->orders);
    }
}
