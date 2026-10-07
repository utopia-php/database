<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Exception\Structure;
use Utopia\Database\Format;
use Utopia\Database\IntegerWidth;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Query\Schema\ColumnType;

final class AttributeStorageTest extends TestCase
{
    private const array STORED_KEYS = ['$id', 'key', 'type', 'size', 'required', 'default', 'signed', 'array', 'format', 'formatOptions', 'filters'];

    /**
     * @return array<string, array{string, ColumnType}>
     */
    public static function storedTypesOfSevenFour(): array
    {
        return [
            'VAR_STRING' => ['string', ColumnType::String],
            'VAR_INTEGER' => ['integer', ColumnType::Integer],
            'VAR_BIGINT' => ['bigint', ColumnType::BigInteger],
            'VAR_FLOAT' => ['double', ColumnType::Double],
            'VAR_BOOLEAN' => ['boolean', ColumnType::Boolean],
            'VAR_DATETIME' => ['datetime', ColumnType::Datetime],
            'VAR_VARCHAR' => ['varchar', ColumnType::Varchar],
            'VAR_TEXT' => ['text', ColumnType::Text],
            'VAR_MEDIUMTEXT' => ['mediumtext', ColumnType::MediumText],
            'VAR_LONGTEXT' => ['longtext', ColumnType::LongText],
            'VAR_ID' => ['id', ColumnType::Id],
            'VAR_OBJECT' => ['object', ColumnType::Object],
            'VAR_VECTOR' => ['vector', ColumnType::Vector],
            'VAR_POINT' => ['point', ColumnType::Point],
            'VAR_LINESTRING' => ['linestring', ColumnType::Linestring],
            'VAR_POLYGON' => ['polygon', ColumnType::Polygon],
        ];
    }

    #[DataProvider('storedTypesOfSevenFour')]
    public function testEverySevenFourStoredTypeHydrates(string $stored, ColumnType $type): void
    {
        $attribute = Attribute::fromDocument(new Document([
            '$id' => 'field',
            'key' => 'field',
            'type' => $stored,
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'format' => '',
            'formatOptions' => [],
            'filters' => [],
        ]));

        $this->assertSame($type, $attribute->type);
        $this->assertSame('field', $attribute->key);
        $this->assertSame($stored, $attribute->toDocument()->getAttribute('type'));
    }

    public function testFloatStoredByEightZeroHydratesAsFloat(): void
    {
        $this->assertSame(ColumnType::Float, Attribute::fromArray(['key' => 'score', 'type' => 'float'])->type);
    }

    public function testCanonicalBigIntegerSpellingHydratesAndPersistsAsLegacySpelling(): void
    {
        $attribute = Attribute::fromArray(['key' => 'total', 'type' => 'biginteger']);

        $this->assertSame(ColumnType::BigInteger, $attribute->type);
        $this->assertSame('bigint', $attribute->toDocument()->getAttribute('type'));
    }

    public function testSevenFourRelationshipShapeHydrates(): void
    {
        $attribute = Attribute::fromDocument(new Document([
            '$id' => 'comments',
            'key' => 'comments',
            'type' => 'relationship',
            'required' => false,
            'default' => null,
            'options' => [
                'relatedCollection' => 'comments',
                'relationType' => 'oneToMany',
                'twoWay' => true,
                'twoWayKey' => 'post',
                'onDelete' => 'cascade',
                'side' => 'parent',
            ],
        ]));

        $this->assertSame(ColumnType::Relationship, $attribute->type);
        $this->assertSame(RelationshipSide::Parent, $attribute->side);
        $this->assertSame('comments', $attribute->relationship?->relatedCollection);
        $this->assertSame(RelationshipType::OneToMany, $attribute->relationship->type);
        $this->assertTrue($attribute->relationship->twoWay);
        $this->assertSame('comments', $attribute->relationship->key);
        $this->assertSame('post', $attribute->relationship->twoWayKey);
        $this->assertSame(RelationshipDeleteAction::Cascade, $attribute->relationship->onDelete);
    }

    public function testRelationshipRoundTripsThroughStorage(): void
    {
        $relationship = Relationship::manyToMany('tags', key: 'tags', twoWay: true, twoWayKey: 'posts', onDelete: RelationshipDeleteAction::SetNull);
        $attribute = Attribute::relationship('tags', $relationship, RelationshipSide::Child);

        $stored = $attribute->toDocument();
        $hydrated = Attribute::fromDocument($stored);

        $this->assertSame($stored->getArrayCopy(), $hydrated->toDocument()->getArrayCopy());
        $this->assertSame(RelationshipSide::Child, $hydrated->side);
        $this->assertSame(RelationshipType::ManyToMany, $hydrated->relationship?->type);
        $this->assertSame(RelationshipDeleteAction::SetNull, $hydrated->relationship->onDelete);
    }

    public function testRelationshipOptionsMayArriveAsDocument(): void
    {
        $attribute = Attribute::fromArray([
            'key' => 'author',
            'type' => 'relationship',
            'options' => new Document([
                'relatedCollection' => 'users',
                'relationType' => 'manyToOne',
                'twoWay' => false,
                'twoWayKey' => 'posts',
                'onDelete' => 'restrict',
                'side' => 'child',
            ]),
        ]);

        $this->assertSame('users', $attribute->relationship?->relatedCollection);
        $this->assertSame(RelationshipSide::Child, $attribute->side);
    }

    public function testRelationshipWithoutSideDefaultsToParent(): void
    {
        $attribute = Attribute::fromArray([
            'key' => 'author',
            'type' => 'relationship',
            'options' => [
                'relatedCollection' => 'users',
                'relationType' => 'oneToOne',
                'twoWay' => false,
                'twoWayKey' => 'post',
                'onDelete' => 'restrict',
            ],
        ]);

        $this->assertSame(RelationshipSide::Parent, $attribute->side);
    }

    public function testRelationshipWithoutOptionsIsRejected(): void
    {
        $this->expectException(Structure::class);

        Attribute::fromArray(['key' => 'author', 'type' => 'relationship']);
    }

    public function testRelationshipWithUnknownSideIsRejected(): void
    {
        $this->expectException(Structure::class);

        Attribute::fromArray([
            'key' => 'author',
            'type' => 'relationship',
            'options' => ['relatedCollection' => 'users', 'relationType' => 'oneToOne', 'side' => 'sideways'],
        ]);
    }

    public function testEmptyStoredFormatHydratesAsNoFormat(): void
    {
        $attribute = Attribute::fromDocument(new Document([
            '$id' => '$createdAt',
            'type' => 'datetime',
            'format' => '',
            'size' => 0,
            'signed' => false,
            'required' => false,
            'default' => null,
            'array' => false,
            'filters' => ['datetime'],
        ]));

        $this->assertNull($attribute->format);
        $this->assertSame('$createdAt', $attribute->key);
        $this->assertNull($attribute->toDocument()->getAttribute('format'));
        $this->assertSame([], $attribute->toDocument()->getAttribute('formatOptions'));
        $this->assertFalse($attribute->signed);
        $this->assertSame(['datetime'], $attribute->filters);
    }

    public function testStatusAndGenericOptionsAreIgnored(): void
    {
        $attribute = Attribute::fromDocument(new Document([
            '$id' => 'name',
            'key' => 'name',
            'type' => 'string',
            'size' => 128,
            'status' => 'available',
            'options' => ['precision' => 2],
            'error' => '',
        ]));

        $stored = $attribute->toDocument();

        $this->assertSame(self::STORED_KEYS, \array_keys($stored->getArrayCopy()));
        $this->assertNull($attribute->relationship);
        $this->assertNull($attribute->side);
        $this->assertSame(128, $attribute->size);
    }

    /**
     * @return array<string, array{array<string, mixed>}>
     */
    public static function internalAttributesOfSevenFour(): array
    {
        return [
            '$id' => [['$id' => '$id', 'type' => 'string', 'size' => 255, 'required' => true, 'signed' => true, 'array' => false, 'filters' => []]],
            '$sequence' => [['$id' => '$sequence', 'type' => 'id', 'size' => 0, 'required' => true, 'signed' => true, 'array' => false, 'filters' => []]],
            '$tenant' => [['$id' => '$tenant', 'type' => 'id', 'size' => 0, 'required' => false, 'default' => null, 'signed' => true, 'array' => false, 'filters' => []]],
            '$updatedAt' => [['$id' => '$updatedAt', 'type' => 'datetime', 'format' => '', 'size' => 0, 'signed' => false, 'required' => false, 'default' => null, 'array' => false, 'filters' => ['datetime']]],
            '$permissions' => [['$id' => '$permissions', 'type' => 'string', 'size' => 1_000_000, 'signed' => true, 'required' => false, 'default' => [], 'array' => false, 'filters' => ['json']]],
        ];
    }

    /**
     * @param  array<string, mixed>  $row
     */
    #[DataProvider('internalAttributesOfSevenFour')]
    public function testSevenFourInternalAttributeRowsRoundTrip(array $row): void
    {
        $stored = Attribute::fromArray($row)->toDocument();

        foreach ($row as $key => $value) {
            if ($key === 'format') {
                $this->assertNull($stored->getAttribute('format'), 'An empty stored format persists as null');

                continue;
            }
            $this->assertSame($value, $stored->getAttribute($key), "Stored '{$key}' changed in the round trip");
        }
    }

    public function testMissingStoredFieldsTakeTheirDefaults(): void
    {
        $attribute = Attribute::fromArray(['$id' => 'name', 'type' => 'string']);

        $this->assertSame('name', $attribute->key);
        $this->assertNull($attribute->size);
        $this->assertFalse($attribute->required);
        $this->assertNull($attribute->default);
        $this->assertTrue($attribute->signed);
        $this->assertFalse($attribute->array);
        $this->assertNull($attribute->format);
        $this->assertSame([], $attribute->filters);
    }

    public function testKeyIsPreferredOverId(): void
    {
        $this->assertSame('key', Attribute::fromArray(['$id' => 'id', 'key' => 'key', 'type' => 'string'])->key);
        $this->assertSame('key', Attribute::fromDocument(new Document(['$id' => 'id', 'key' => 'key', 'type' => 'string']))->key);
    }

    public function testFormatAndOptionsHydrate(): void
    {
        $attribute = Attribute::fromArray([
            'key' => 'range',
            'type' => 'integer',
            'format' => 'intRange',
            'formatOptions' => ['min' => 1, 'max' => 10],
        ]);

        $this->assertSame('intRange', $attribute->format?->name);
        $this->assertSame(['min' => 1, 'max' => 10], $attribute->format->options);
    }

    public function testFormatOptionsMayArriveAsDocument(): void
    {
        $attribute = Attribute::fromDocument(new Document([
            '$id' => 'range',
            'type' => 'integer',
            'format' => 'intRange',
            'formatOptions' => new Document(['min' => 1]),
        ]));

        $this->assertSame(['min' => 1], $attribute->format?->options);
    }

    public function testStoredSizeHydratesAndNumericStringsAreAccepted(): void
    {
        $this->assertSame(8, Attribute::fromArray(['key' => 'count', 'type' => 'integer', 'size' => 8])->size);
        $this->assertSame(IntegerWidth::Bits64, Attribute::fromArray(['key' => 'count', 'type' => 'integer', 'size' => '8'])->width());
    }

    /**
     * @return array<string, array{\Closure(): Attribute}>
     */
    public static function factories(): array
    {
        return [
            'string' => [static fn (): Attribute => Attribute::string('a', 10, true, 'x', true, new Format('email', ['strict' => true]), ['lowercase'])],
            'varchar' => [static fn (): Attribute => Attribute::varchar('a', 20)],
            'text' => [static fn (): Attribute => Attribute::text('a')],
            'mediumText' => [static fn (): Attribute => Attribute::mediumText('a', 300)],
            'longText' => [static fn (): Attribute => Attribute::longText('a')],
            'integer' => [static fn (): Attribute => Attribute::integer('a', default: 3)],
            'integer64' => [static fn (): Attribute => Attribute::integer('a', signed: false, width: IntegerWidth::Bits64)],
            'bigInteger' => [static fn (): Attribute => Attribute::bigInteger('a', default: '42')],
            'float' => [static fn (): Attribute => Attribute::float('a', default: 1.25)],
            'double' => [static fn (): Attribute => Attribute::double('a', array: true, default: [1.5])],
            'boolean' => [static fn (): Attribute => Attribute::boolean('a', default: true)],
            'datetime' => [static fn (): Attribute => Attribute::datetime('a', required: true)],
            'point' => [static fn (): Attribute => Attribute::point('a', default: [0.0, 0.0])],
            'lineString' => [static fn (): Attribute => Attribute::lineString('a')],
            'polygon' => [static fn (): Attribute => Attribute::polygon('a')],
            'vector' => [static fn (): Attribute => Attribute::vector('a', 3)],
            'object' => [static fn (): Attribute => Attribute::object('a')],
            'id' => [static fn (): Attribute => Attribute::id('a', default: 7)],
        ];
    }

    /**
     * @param  \Closure(): Attribute  $factory
     */
    #[DataProvider('factories')]
    public function testFactoryOutputRoundTripsThroughStorage(\Closure $factory): void
    {
        $attribute = $factory();
        $stored = $attribute->toDocument();

        $this->assertSame(self::STORED_KEYS, \array_keys($stored->getArrayCopy()));

        $fromDocument = Attribute::fromDocument($stored);
        $fromArray = Attribute::fromArray($stored->getArrayCopy());

        $this->assertSame($stored->getArrayCopy(), $fromDocument->toDocument()->getArrayCopy());
        $this->assertSame($stored->getArrayCopy(), $fromArray->toDocument()->getArrayCopy());
        $this->assertSame($attribute->type, $fromDocument->type);
        $this->assertSame($attribute->size, $fromDocument->size);
        $this->assertSame($attribute->format?->name, $fromDocument->format?->name);
    }

    /**
     * @return array<string, array{mixed}>
     */
    public static function rejectedTypes(): array
    {
        return [
            'tuple' => ['tuple'],
            'uuid7 (sequence type, never an attribute type)' => ['uuid7'],
            'json' => ['json'],
            'unknown' => ['geometry'],
            'empty' => [''],
            'missing' => [null],
            'non-string' => [42],
            'non-attribute enum' => [ColumnType::Serial],
        ];
    }

    #[DataProvider('rejectedTypes')]
    public function testTypesOutsideTheAttributeTypesAreRejected(mixed $type): void
    {
        $this->expectException(Structure::class);

        Attribute::fromArray(['key' => 'field', 'type' => $type]);
    }

    public function testFromDocumentRejectsTypesOutsideTheAttributeTypes(): void
    {
        $this->expectException(Structure::class);

        Attribute::fromDocument(new Document(['$id' => 'field', 'type' => 'tuple']));
    }

    public function testFromArrayRejectsTupleWithoutAnyOtherField(): void
    {
        $this->expectException(Structure::class);

        Attribute::fromArray(['type' => 'tuple']);
    }

    public function testColumnTypeInstanceInStorageIsTolerated(): void
    {
        $this->assertSame(ColumnType::Polygon, Attribute::fromArray(['key' => 'area', 'type' => ColumnType::Polygon])->type);
    }
}
