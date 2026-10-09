<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Structure;
use Utopia\Database\Filter;
use Utopia\Database\Validator\BigInt;
use Utopia\Query\Schema\ColumnType;

final class AttributeTest extends TestCase
{
    public function testConstructorIsNotCallableFromOutside(): void
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Call to private Utopia\Database\Attribute::__construct()');

        $attribute = new Attribute('name', ColumnType::String, 10, false, null, true, false, null, [], null, null); // @phpstan-ignore new.privateConstructor
    }

    public function testTypesListsTheEighteenAttributeTypes(): void
    {
        $this->assertCount(18, \array_filter(ColumnType::cases(), $this->hydratesAsAttributeType(...)));
        $this->assertSame(Attribute::TYPES, \array_values(\array_unique(Attribute::TYPES, \SORT_REGULAR)));
        $this->assertContains(ColumnType::Relationship, Attribute::TYPES);
        $this->assertNotContains(ColumnType::Tuple, Attribute::TYPES);
        $this->assertNotContains(ColumnType::Uuid7, Attribute::TYPES);
    }

    /**
     * @return array<string, array{Attribute, bool, bool, bool}>
     */
    public static function predicates(): array
    {
        return [
            'string' => [Attribute::string('a'), false, false, false],
            'integer' => [Attribute::integer('a'), false, true, true],
            'bigInteger' => [Attribute::bigInteger('a'), false, true, true],
            'float' => [Attribute::float('a'), false, true, false],
            'double' => [Attribute::double('a'), false, true, false],
            'boolean' => [Attribute::boolean('a'), false, false, false],
            'datetime' => [Attribute::datetime('a'), false, false, false],
            'point' => [Attribute::point('a'), true, false, false],
            'lineString' => [Attribute::lineString('a'), true, false, false],
            'polygon' => [Attribute::polygon('a'), true, false, false],
            'vector' => [Attribute::vector('a', 3), false, false, false],
            'object' => [Attribute::object('a'), false, false, false],
            'id' => [Attribute::id('a'), false, false, false],
        ];
    }

    #[DataProvider('predicates')]
    public function testTypePredicates(Attribute $attribute, bool $spatial, bool $numeric, bool $integer): void
    {
        $this->assertSame($spatial, $attribute->isSpatial());
        $this->assertSame($numeric, $attribute->isNumeric());
        $this->assertSame($integer, $attribute->isInteger());
    }

    /**
     * @return array<string, array{Attribute, int|float|string, int|float|string}>
     */
    public static function bounds(): array
    {
        return [
            'signed integer' => [Attribute::integer('a'), Database::MIN_INT, Database::MAX_INT],
            'unsigned integer' => [Attribute::integer('a', signed: false), 0, Database::MAX_INT],
            'signed bigInteger' => [Attribute::bigInteger('a'), \PHP_INT_MIN, Database::MAX_BIG_INT],
            'unsigned bigInteger' => [Attribute::bigInteger('a', signed: false), 0, BigInt::UNSIGNED_MAX],
            'signed float' => [Attribute::float('a'), -Database::MAX_DOUBLE, Database::MAX_DOUBLE],
            'unsigned double' => [Attribute::double('a', signed: false), 0, Database::MAX_DOUBLE],
        ];
    }

    #[DataProvider('bounds')]
    public function testNumericBounds(Attribute $attribute, int|float|string $min, int|float|string $max): void
    {
        $bounds = $attribute->bounds();

        $this->assertNotNull($bounds);
        $this->assertSame($min, $bounds->min);
        $this->assertSame($max, $bounds->max);
    }

    public function testNonNumericTypesHaveNoBounds(): void
    {
        $this->assertNull(Attribute::string('a')->bounds());
        $this->assertNull(Attribute::boolean('a')->bounds());
        $this->assertNull(Attribute::datetime('a')->bounds());
    }

    public function testResolvedSizeUsesTheEngineMaximumForSizelessText(): void
    {
        $this->assertSame(Database::MAX_TEXT_BYTES, Attribute::text('a')->resolvedSize());
        $this->assertSame(Database::MAX_MEDIUMTEXT_BYTES, Attribute::mediumText('a')->resolvedSize());
        $this->assertSame(Database::MAX_LONGTEXT_BYTES, Attribute::longText('a')->resolvedSize());
    }

    public function testResolvedSizeUsesTheDeclaredSize(): void
    {
        $this->assertSame(500, Attribute::text('a', 500)->resolvedSize());
        $this->assertSame(64, Attribute::varchar('a', 64)->resolvedSize());
        $this->assertSame(8, Attribute::fromArray(['key' => 'a', 'type' => 'integer', 'size' => 8])->resolvedSize());
    }

    public function testResolvedSizeIsZeroForSizelessNonTextTypes(): void
    {
        $this->assertSame(0, Attribute::boolean('a')->resolvedSize());
        $this->assertSame(0, Attribute::integer('a')->resolvedSize());
    }

    public function testWidthIsOnlyDefinedForIntegers(): void
    {
        $this->assertNull(Attribute::bigInteger('a')->width());
        $this->assertNull(Attribute::string('a')->width());
        $this->assertNull(Attribute::double('a')->width());
    }

    public function testWithFiltersReplacesFiltersOnly(): void
    {
        $original = Attribute::string('name', 32, required: true, default: 'x');

        $filtered = $original->withFilters([Filter::Json, 'encrypt']);

        $this->assertSame(['json', 'encrypt'], $filtered->filters);
        $this->assertSame([], $original->filters);
        $this->assertSame('name', $filtered->key);
        $this->assertSame(32, $filtered->size);
        $this->assertTrue($filtered->required);
        $this->assertSame('x', $filtered->default);
        $this->assertNotSame($original, $filtered);
    }

    public function testWithFiltersCanClearFilters(): void
    {
        $this->assertSame([], Attribute::string('a', filters: ['encrypt'])->withFilters([])->filters);
    }

    public function testWithFiltersKeepsTheTypeFilter(): void
    {
        $this->assertSame(['datetime'], Attribute::datetime('a')->withFilters([])->filters);
        $this->assertSame(['vector', 'encrypt'], Attribute::vector('a', 3)->withFilters(['encrypt'])->filters);
        $this->assertSame(['encrypt', 'object'], Attribute::object('a')->withFilters(['encrypt', Filter::Object])->filters);
    }

    public function testIsRelationshipReadsTheRawStoredType(): void
    {
        $this->assertTrue(Attribute::isRelationship(new Document(['type' => 'relationship'])));
        $this->assertTrue(Attribute::isRelationship(new Document(['type' => ColumnType::Relationship])));
        $this->assertFalse(Attribute::isRelationship(new Document(['type' => 'string'])));
        $this->assertFalse(Attribute::isRelationship(new Document(['type' => 'bigint'])));
        $this->assertFalse(Attribute::isRelationship(new Document([])));
        $this->assertFalse(Attribute::isRelationship(Attribute::string('relationship')->toDocument()));
    }

    public function testStoredTypeUsesTheLegacyBigIntegerSpelling(): void
    {
        $this->assertSame('bigint', Attribute::storedType(ColumnType::BigInteger));
        $this->assertSame('integer', Attribute::storedType(ColumnType::Integer));
        $this->assertSame('linestring', Attribute::storedType(ColumnType::Linestring));
    }

    public function testTypeFromStoredMapsBothBigIntegerSpellings(): void
    {
        $this->assertSame(ColumnType::BigInteger, Attribute::typeFromStored('bigint'));
        $this->assertSame(ColumnType::BigInteger, Attribute::typeFromStored('biginteger'));
        $this->assertSame(ColumnType::Double, Attribute::typeFromStored('double'));
    }

    public function testEveryTypeRoundTripsThroughItsStoredSpelling(): void
    {
        foreach (Attribute::TYPES as $type) {
            $this->assertSame($type, Attribute::typeFromStored(Attribute::storedType($type)));
        }
    }

    public function testTypeFromStoredRejectsNonAttributeTypes(): void
    {
        $this->expectException(Structure::class);
        $this->expectExceptionMessage('Unknown attribute type: tuple');

        Attribute::typeFromStored('tuple');
    }

    public function testTypeFromStoredRejectsUnknownStrings(): void
    {
        $this->expectException(Structure::class);

        Attribute::typeFromStored('BIGINT');
    }

    private function hydratesAsAttributeType(ColumnType $type): bool
    {
        try {
            Attribute::typeFromStored($type->value);

            return true;
        } catch (Structure) {
            return false;
        }
    }
}
