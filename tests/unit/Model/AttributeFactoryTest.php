<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Exception\Structure;
use Utopia\Database\Filter;
use Utopia\Database\Format;
use Utopia\Database\IntegerWidth;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Query\Schema\ColumnType;

final class AttributeFactoryTest extends TestCase
{
    public function testDatetimePersistsUnsignedWithDatetimeFilter(): void
    {
        $stored = Attribute::datetime('publishedAt', required: true)->toDocument();

        $this->assertSame('datetime', $stored->getAttribute('type'));
        $this->assertFalse($stored->getAttribute('signed'));
        $this->assertSame(['datetime'], $stored->getAttribute('filters'));
        $this->assertSame(0, $stored->getAttribute('size'));
        $this->assertTrue($stored->getAttribute('required'));
    }

    public function testDatetimeArrayKeepsTheDatetimeFilter(): void
    {
        $attribute = Attribute::datetime('history', array: true, default: ['2024-01-01T00:00:00.000+00:00']);

        $this->assertTrue($attribute->array);
        $this->assertSame([Filter::Datetime->value], $attribute->filters);
        $this->assertSame(['2024-01-01T00:00:00.000+00:00'], $attribute->default);
    }

    /**
     * @return array<string, array{\Closure(): Attribute, string}>
     */
    public static function spatialFactories(): array
    {
        return [
            'point' => [static fn (): Attribute => Attribute::point('location'), 'point'],
            'lineString' => [static fn (): Attribute => Attribute::lineString('location'), 'linestring'],
            'polygon' => [static fn (): Attribute => Attribute::polygon('location'), 'polygon'],
        ];
    }

    /**
     * @param  \Closure(): Attribute  $factory
     */
    #[DataProvider('spatialFactories')]
    public function testSpatialPersistsScalarWithItsTypeFilter(\Closure $factory, string $type): void
    {
        $stored = $factory()->toDocument();

        $this->assertSame($type, $stored->getAttribute('type'));
        $this->assertFalse($stored->getAttribute('array'));
        $this->assertSame([$type], $stored->getAttribute('filters'));
        $this->assertSame(0, $stored->getAttribute('size'));
        $this->assertTrue($stored->getAttribute('signed'));
    }

    public function testSpatialKeepsItsDefault(): void
    {
        $attribute = Attribute::point('location', required: true, default: [1.5, 2.5]);

        $this->assertSame([1.5, 2.5], $attribute->default);
        $this->assertTrue($attribute->required);
    }

    public function testVectorPersistsDimensionsAsSize(): void
    {
        $stored = Attribute::vector('embedding', 1536, default: [0.0, 1.0])->toDocument();

        $this->assertSame('vector', $stored->getAttribute('type'));
        $this->assertSame(1536, $stored->getAttribute('size'));
        $this->assertSame(['vector'], $stored->getAttribute('filters'));
        $this->assertFalse($stored->getAttribute('array'));
        $this->assertSame([0.0, 1.0], $stored->getAttribute('default'));
    }

    public function testObjectPersistsScalarWithObjectFilter(): void
    {
        $stored = Attribute::object('metadata', default: ['a' => 1])->toDocument();

        $this->assertSame('object', $stored->getAttribute('type'));
        $this->assertSame(['object'], $stored->getAttribute('filters'));
        $this->assertFalse($stored->getAttribute('array'));
        $this->assertSame(0, $stored->getAttribute('size'));
        $this->assertSame(['a' => 1], $stored->getAttribute('default'));
    }

    public function testIntegerDefaultsToThirtyTwoBitsPersistedAsSizeZero(): void
    {
        $attribute = Attribute::integer('count');

        $this->assertNull($attribute->size);
        $this->assertSame(IntegerWidth::Bits32, $attribute->width());
        $this->assertSame(0, $attribute->toDocument()->getAttribute('size'));
        $this->assertTrue($attribute->signed);
    }

    public function testIntegerSixtyFourBitsPersistsAsSizeEight(): void
    {
        $attribute = Attribute::integer('count', width: IntegerWidth::Bits64, signed: false, default: 5);

        $this->assertSame(8, $attribute->size);
        $this->assertSame(IntegerWidth::Bits64, $attribute->width());
        $this->assertSame(8, $attribute->toDocument()->getAttribute('size'));
        $this->assertFalse($attribute->signed);
        $this->assertSame(5, $attribute->default);
    }

    /**
     * @return array<string, array{\Closure(): Attribute, string}>
     */
    public static function sizelessFactories(): array
    {
        return [
            'boolean' => [static fn (): Attribute => Attribute::boolean('flag'), 'boolean'],
            'double' => [static fn (): Attribute => Attribute::double('score'), 'double'],
            'float' => [static fn (): Attribute => Attribute::float('score'), 'float'],
            'id' => [static fn (): Attribute => Attribute::id('reference'), 'id'],
            'bigInteger' => [static fn (): Attribute => Attribute::bigInteger('total'), 'bigint'],
        ];
    }

    /**
     * @param  \Closure(): Attribute  $factory
     */
    #[DataProvider('sizelessFactories')]
    public function testSizelessTypesPersistSizeZero(\Closure $factory, string $type): void
    {
        $attribute = $factory();
        $stored = $attribute->toDocument();

        $this->assertNull($attribute->size);
        $this->assertSame($type, $stored->getAttribute('type'));
        $this->assertSame(0, $stored->getAttribute('size'));
        $this->assertSame([], $stored->getAttribute('filters'));
        $this->assertTrue($stored->getAttribute('signed'));
    }

    /**
     * @return array<string, array{\Closure(): Attribute, string, int}>
     */
    public static function stringFactories(): array
    {
        return [
            'string' => [static fn (): Attribute => Attribute::string('name'), 'string', Database::LENGTH_KEY],
            'varchar' => [static fn (): Attribute => Attribute::varchar('name', 64), 'varchar', 64],
            'text' => [static fn (): Attribute => Attribute::text('name'), 'text', 0],
            'mediumText' => [static fn (): Attribute => Attribute::mediumText('name'), 'mediumtext', 0],
            'longText' => [static fn (): Attribute => Attribute::longText('name', 1024), 'longtext', 1024],
        ];
    }

    /**
     * @param  \Closure(): Attribute  $factory
     */
    #[DataProvider('stringFactories')]
    public function testStringFamilyPersistsSigned(\Closure $factory, string $type, int $size): void
    {
        $stored = $factory()->toDocument();

        $this->assertSame($type, $stored->getAttribute('type'));
        $this->assertTrue($stored->getAttribute('signed'));
        $this->assertSame($size, $stored->getAttribute('size'));
        $this->assertFalse($stored->getAttribute('array'));
        $this->assertSame([], $stored->getAttribute('filters'));
    }

    public function testExplicitZeroSizeMeansNoSize(): void
    {
        $attribute = Attribute::text('body', 0);

        $this->assertNull($attribute->size);
        $this->assertSame(0, $attribute->toDocument()->getAttribute('size'));
    }

    public function testStringCarriesEveryArgument(): void
    {
        $attribute = Attribute::string(
            'email',
            size: 320,
            required: true,
            default: 'user@example.com',
            array: true,
            format: new Format('email', ['allowPlus' => true]),
            filters: [Filter::Json, 'lowercase'],
        );

        $this->assertSame('email', $attribute->key);
        $this->assertSame(ColumnType::String, $attribute->type);
        $this->assertSame(320, $attribute->size);
        $this->assertTrue($attribute->required);
        $this->assertSame('user@example.com', $attribute->default);
        $this->assertTrue($attribute->array);
        $this->assertSame('email', $attribute->format?->name);
        $this->assertSame(['allowPlus' => true], $attribute->format->options);
        $this->assertSame(['json', 'lowercase'], $attribute->filters);
        $this->assertNull($attribute->relationship);
        $this->assertNull($attribute->side);
    }

    public function testEveryFactoryProducesItsColumnType(): void
    {
        $this->assertSame(ColumnType::String, Attribute::string('a')->type);
        $this->assertSame(ColumnType::Varchar, Attribute::varchar('a')->type);
        $this->assertSame(ColumnType::Text, Attribute::text('a')->type);
        $this->assertSame(ColumnType::MediumText, Attribute::mediumText('a')->type);
        $this->assertSame(ColumnType::LongText, Attribute::longText('a')->type);
        $this->assertSame(ColumnType::Integer, Attribute::integer('a')->type);
        $this->assertSame(ColumnType::BigInteger, Attribute::bigInteger('a')->type);
        $this->assertSame(ColumnType::Float, Attribute::float('a')->type);
        $this->assertSame(ColumnType::Double, Attribute::double('a')->type);
        $this->assertSame(ColumnType::Boolean, Attribute::boolean('a')->type);
        $this->assertSame(ColumnType::Datetime, Attribute::datetime('a')->type);
        $this->assertSame(ColumnType::Point, Attribute::point('a')->type);
        $this->assertSame(ColumnType::Linestring, Attribute::lineString('a')->type);
        $this->assertSame(ColumnType::Polygon, Attribute::polygon('a')->type);
        $this->assertSame(ColumnType::Vector, Attribute::vector('a', 3)->type);
        $this->assertSame(ColumnType::Object, Attribute::object('a')->type);
        $this->assertSame(ColumnType::Id, Attribute::id('a')->type);
    }

    public function testNumericFactoriesKeepSignedAndDefault(): void
    {
        $double = Attribute::double('score', required: true, default: 1.5, signed: false, array: true);

        $this->assertFalse($double->signed);
        $this->assertSame(1.5, $double->default);
        $this->assertTrue($double->required);
        $this->assertTrue($double->array);

        $big = Attribute::bigInteger('total', default: '9223372036854775808', signed: false);

        $this->assertSame('9223372036854775808', $big->default);
        $this->assertFalse($big->signed);
    }

    public function testRelationshipPersistsSevenXOptionsShapePlusSide(): void
    {
        $relationship = Relationship::oneToMany('comments', key: 'comments', twoWay: true, twoWayKey: 'post', onDelete: RelationshipDeleteAction::Cascade);
        $stored = Attribute::relationship('comments', $relationship, RelationshipSide::Parent)->toDocument();

        $this->assertSame('relationship', $stored->getAttribute('type'));
        $this->assertSame('comments', $stored->getAttribute('key'));
        $this->assertFalse($stored->getAttribute('required'));
        $this->assertNull($stored->getAttribute('default'));
        $this->assertSame([
            'relatedCollection' => 'comments',
            'relationType' => 'oneToMany',
            'twoWay' => true,
            'twoWayKey' => 'post',
            'onDelete' => 'cascade',
            'side' => 'parent',
        ], $stored->getAttribute('options'));
    }

    public function testRelationshipAdoptsTheAttributeKeyWhenTheRelationshipHasNone(): void
    {
        $attribute = Attribute::relationship('author', Relationship::manyToOne('users'), RelationshipSide::Child);

        $this->assertSame(ColumnType::Relationship, $attribute->type);
        $this->assertSame('author', $attribute->relationship?->key);
        $this->assertSame(RelationshipType::ManyToOne, $attribute->relationship->type);
        $this->assertSame(RelationshipSide::Child, $attribute->side);
        $this->assertNull($attribute->size);
        $this->assertSame([], $attribute->filters);
    }

    public function testRelationshipRejectsAMismatchedKey(): void
    {
        $this->expectException(Structure::class);

        Attribute::relationship('author', Relationship::manyToOne('users', key: 'writer'), RelationshipSide::Parent);
    }
}
