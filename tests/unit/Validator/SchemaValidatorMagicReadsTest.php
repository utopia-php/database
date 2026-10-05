<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\CountingAttribute;
use Tests\Unit\CountingIndex;
use Tests\Unit\MagicAccessAssertions;
use Tests\Unit\MagicAccessRecorder;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Attribute as AttributeValidator;
use Utopia\Database\Validator\Index as IndexValidator;
use Utopia\Database\Validator\IndexDependency;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\Order;

final class SchemaValidatorMagicReadsTest extends TestCase
{
    use MagicAccessAssertions;

    /**
     * @return iterable<string, array{Index, bool}>
     */
    public static function indexes(): iterable
    {
        foreach (IndexType::cases() as $type) {
            yield $type->value => match ($type) {
                IndexType::Key => [Index::key(key: 'by_title_and_tags', attributes: ['title', 'tags'], lengths: [64, 32], orders: [Order::Desc, null]), true],
                IndexType::Index => [Index::index(key: 'by_count', attributes: ['count']), false],
                IndexType::Unique => [Index::unique(key: 'unique_title', attributes: ['title', 'count'], lengths: [64], orders: [Order::Asc, Order::Desc]), true],
                IndexType::Fulltext => [Index::fullText(key: 'search_title', attributes: ['title']), true],
                IndexType::Spatial => [Index::spatial(key: 'by_location', attributes: ['location'], orders: [Order::Desc]), true],
                IndexType::Object => [Index::object(key: 'by_data', attributes: ['data']), true],
                IndexType::HnswEuclidean => [Index::hnswEuclidean(key: 'nearest_euclidean', attributes: ['embedding']), true],
                IndexType::HnswCosine => [Index::hnswCosine(key: 'nearest_cosine', attributes: ['embedding']), true],
                IndexType::HnswDot => [Index::hnswDot(key: 'nearest_dot', attributes: ['embedding']), true],
                IndexType::Trigram => [Index::trigram(key: 'similar_title', attributes: ['title']), true],
                IndexType::Ttl => [Index::ttl(key: 'expiry', attributes: ['expiresAt'], ttl: 3600), true],
            };
        }

        yield 'key on an object path' => [Index::key(key: 'by_data_name', attributes: ['data.name']), true];
    }

    /**
     * @return iterable<string, array{Attribute}>
     */
    public static function attributes(): iterable
    {
        foreach (Attribute::TYPES as $type) {
            yield $type->value => [match ($type) {
                ColumnType::String => new Attribute(key: 'subtitle', type: $type, size: 128, default: 'none'),
                ColumnType::Varchar => new Attribute(key: 'code', type: $type, size: 64, default: 'A1'),
                ColumnType::Text => new Attribute(key: 'summary', type: $type, size: 65535, default: 'none'),
                ColumnType::MediumText => new Attribute(key: 'body', type: $type, size: 16777215),
                ColumnType::LongText => new Attribute(key: 'archive', type: $type, size: 4294967295),
                ColumnType::Integer => new Attribute(key: 'pages', type: $type, size: 4, default: 100),
                ColumnType::BigInteger => new Attribute(key: 'views', type: $type, default: 9007199254740993, signed: false),
                ColumnType::Float => new Attribute(key: 'weight', type: $type, default: 1.5),
                ColumnType::Double => new Attribute(key: 'price', type: $type, default: 2.5),
                ColumnType::Boolean => new Attribute(key: 'lent', type: $type, default: false),
                ColumnType::Datetime => new Attribute(key: 'printedAt', type: $type, default: '2000-01-01T00:00:00.000+00:00', filters: ['datetime']),
                ColumnType::Id => new Attribute(key: 'shelf', type: $type),
                ColumnType::Relationship => Attribute::relationship(key: 'author', options: [
                    'relatedCollection' => 'authors',
                    'relationType' => RelationType::ManyToOne->value,
                    'twoWay' => true,
                    'twoWayKey' => 'books',
                    'onDelete' => ForeignKeyAction::SetNull->value,
                    'side' => RelationSide::Parent->value,
                ]),
                ColumnType::Object => new Attribute(key: 'metadata', type: $type, default: ['edition' => 1]),
                ColumnType::Point => new Attribute(key: 'origin', type: $type, default: [1.0, 2.0]),
                ColumnType::Linestring => new Attribute(key: 'route', type: $type, default: [[0.0, 0.0], [1.0, 1.0]]),
                ColumnType::Polygon => new Attribute(key: 'area', type: $type, default: [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]),
                ColumnType::Vector => new Attribute(key: 'embedding', type: $type, size: 3, default: [1.0, 2.0, 3.0]),
            }];
        }

        yield 'array with an array default' => [new Attribute(key: 'labels', type: ColumnType::String, size: 32, default: ['new'], array: true)];
        yield 'json document default' => [new Attribute(key: 'settings', type: ColumnType::String, size: 1024, default: ['theme' => 'dark'], filters: ['json'])];
    }

    #[DataProvider('indexes')]
    public function testIndexValidation(Index $index, bool $valid): void
    {
        $recorder = new MagicAccessRecorder();
        $candidate = CountingIndex::of($index, $recorder);
        $attributes = $this->counting($recorder, [
            new Attribute(key: 'title', type: ColumnType::String, size: 128),
            new Attribute(key: 'tags', type: ColumnType::String, size: 32, array: true),
            new Attribute(key: 'count', type: ColumnType::Integer, size: 4),
            new Attribute(key: 'location', type: ColumnType::Point, required: true),
            new Attribute(key: 'data', type: ColumnType::Object),
            new Attribute(key: 'embedding', type: ColumnType::Vector, size: 3),
            new Attribute(key: 'expiresAt', type: ColumnType::Datetime, filters: ['datetime']),
        ]);
        $existing = [
            CountingIndex::of(Index::key(key: 'by_count', attributes: ['count'], orders: [Order::Asc]), $recorder),
            CountingIndex::of(Index::unique(key: 'unique_count', attributes: ['count'], orders: [Order::Desc]), $recorder),
        ];

        $recorder->start();
        $validator = new IndexValidator(
            attributes: $attributes,
            indexes: $existing,
            maxLength: 3072,
            reservedKeys: ['_id', '_uid'],
            supportForArrayIndexes: true,
            supportForSpatialIndexNull: false,
            supportForSpatialIndexOrder: true,
            supportForVectorIndexes: true,
            supportForAttributes: true,
            supportForMultipleFulltextIndexes: false,
            supportForIdenticalIndexes: false,
            supportForObjectIndexes: true,
            supportForTrigramIndexes: true,
            supportForSpatialIndexes: true,
            supportForKeyIndexes: true,
            supportForUniqueIndexes: true,
            supportForFulltextIndexes: true,
            supportForTTLIndexes: true,
            supportForObjects: true,
        );
        $result = $validator->isValid($candidate);
        $recorder->stop();

        $this->assertSame($valid, $result, $validator->getDescription());
        $this->assertNoMagicAccess($recorder, 'Validating the '.$index->getKey().' '.$index->getType()->value.' index');
    }

    #[DataProvider('attributes')]
    public function testAttributeValidation(Attribute $attribute): void
    {
        $recorder = new MagicAccessRecorder();
        $candidate = CountingAttribute::of($attribute, $recorder);
        $existing = $this->counting($recorder, [
            new Attribute(key: 'title', type: ColumnType::String, size: 128),
            new Attribute(key: 'tags', type: ColumnType::String, size: 32, array: true),
        ]);
        $schema = $this->counting($recorder, [new Attribute(key: 'legacy', type: ColumnType::String, size: 64)]);

        $recorder->start();
        $validator = new AttributeValidator(
            attributes: $existing,
            schemaAttributes: $schema,
            maxAttributes: 1017,
            maxWidth: 65535,
            maxStringLength: 1073741824,
            maxVarcharLength: 16381,
            maxIntLength: 4294967295,
            maxBigIntLength: \PHP_INT_MAX,
            supportForSchemaAttributes: true,
            supportForVectors: true,
            supportForSpatialAttributes: true,
            supportForObject: true,
            supportUnsignedBigInt: true,
            attributeCountCallback: static fn (Document $attribute): int => 3,
            attributeWidthCallback: static fn (Document $attribute): int => 1024,
            filterCallback: static fn (string $key): string => $key,
        );
        $result = $validator->isValid($candidate);
        $recorder->stop();

        $this->assertTrue($result, $validator->getDescription());
        $this->assertNoMagicAccess($recorder, 'Validating the '.$attribute->getKey().' '.$attribute->getType()->value.' attribute');
    }

    public function testIndexDependencyValidation(): void
    {
        $recorder = new MagicAccessRecorder();
        $tags = CountingAttribute::of(new Attribute(key: 'tags', type: ColumnType::String, size: 32, array: true), $recorder);
        $labels = CountingAttribute::of(new Attribute(key: 'labels', type: ColumnType::String, size: 32, array: true), $recorder);
        $title = CountingAttribute::of(new Attribute(key: 'title', type: ColumnType::String, size: 128), $recorder);
        $indexes = [
            CountingIndex::of(Index::key(key: 'by_title', attributes: ['title'], lengths: [64]), $recorder),
            CountingIndex::of(Index::key(key: 'by_tags', attributes: ['tags'], lengths: [32]), $recorder),
        ];

        $recorder->start();
        $validator = new IndexDependency($indexes, true);
        $results = [$validator->isValid($tags), $validator->isValid($labels), $validator->isValid($title)];
        $recorder->stop();

        $this->assertSame([false, true, true], $results);
        $this->assertNoMagicAccess($recorder, 'Index dependency validation');
    }

    /**
     * @param  list<Attribute>  $attributes
     * @return list<CountingAttribute>
     */
    private function counting(MagicAccessRecorder $recorder, array $attributes): array
    {
        return \array_map(static fn (Attribute $attribute): CountingAttribute => CountingAttribute::of($attribute, $recorder), $attributes);
    }
}
