<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Attribute\Update;
use Utopia\Database\Exception\Structure;
use Utopia\Database\Filter;
use Utopia\Database\Format;
use Utopia\Database\IntegerWidth;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\Unchanged;
use Utopia\Query\Schema\ColumnType;

final class AttributeUpdateTest extends TestCase
{
    public function testDefaultsLeaveEverythingUnchanged(): void
    {
        $update = new Update();

        $this->assertTrue($update->isEmpty());
        $this->assertFalse($update->changesDefault());
        $this->assertFalse($update->changesFormat());
        $this->assertSame(Unchanged::Value, $update->default);
        $this->assertSame(Unchanged::Value, $update->format);
    }

    public function testNullDefaultIsAChange(): void
    {
        $update = new Update(default: null);

        $this->assertTrue($update->changesDefault());
        $this->assertFalse($update->isEmpty());
    }

    public function testAnySingleFieldMakesTheUpdateNonEmpty(): void
    {
        $updates = [
            new Update(type: ColumnType::Text),
            new Update(size: 10),
            new Update(required: false),
            new Update(default: 'x'),
            new Update(signed: false),
            new Update(array: false),
            new Update(format: new Format('email')),
            new Update(format: null),
            new Update(filters: []),
            new Update(key: 'renamed'),
        ];

        foreach ($updates as $update) {
            $this->assertFalse($update->isEmpty());
        }
    }

    public function testEmptyUpdateKeepsEveryField(): void
    {
        $original = Attribute::string('name', 64, required: true, default: 'x', array: true, format: new Format('email', ['a' => 1]), filters: ['lowercase']);

        $updated = $original->apply(new Update());

        $this->assertSame($original->toDocument()->getArrayCopy(), $updated->toDocument()->getArrayCopy());
    }

    public function testApplyChangesOnlyTheGivenFields(): void
    {
        $original = Attribute::string('name', 64, required: true, default: 'x', filters: ['lowercase']);

        $updated = $original->apply(new Update(size: 128, required: false));

        $this->assertSame(128, $updated->size);
        $this->assertFalse($updated->required);
        $this->assertSame('x', $updated->default);
        $this->assertSame(['lowercase'], $updated->filters);
        $this->assertSame('name', $updated->key);
        $this->assertSame(ColumnType::String, $updated->type);
    }

    public function testZeroSizeClearsTheSize(): void
    {
        $this->assertNull(Attribute::text('body', 500)->apply(new Update(size: 0))->size);
    }

    public function testApplyLeavesTheOriginalUntouched(): void
    {
        $original = Attribute::string('name', 64);

        $original->apply(new Update(size: 128, key: 'title', default: 'y'));

        $this->assertSame(64, $original->size);
        $this->assertSame('name', $original->key);
        $this->assertNull($original->default);
    }

    public function testNullDefaultClearsTheDefault(): void
    {
        $updated = Attribute::integer('count', default: 5)->apply(new Update(default: null));

        $this->assertNull($updated->default);
    }

    public function testDefaultCanBeSet(): void
    {
        $updated = Attribute::integer('count')->apply(new Update(default: 7));

        $this->assertSame(7, $updated->default);
    }

    public function testKeyRenames(): void
    {
        $this->assertSame('title', Attribute::string('name')->apply(new Update(key: 'title'))->key);
    }

    public function testFiltersAcceptEnumCasesAndStrings(): void
    {
        $updated = Attribute::string('name')->apply(new Update(filters: [Filter::Json, 'encrypt']));

        $this->assertSame(['json', 'encrypt'], $updated->filters);
    }

    public function testEmptyFiltersClearTheFilters(): void
    {
        $this->assertSame([], Attribute::string('name', filters: ['lowercase'])->apply(new Update(filters: []))->filters);
    }

    public function testClearingFiltersKeepsTheTypeFilter(): void
    {
        $this->assertSame(['datetime'], Attribute::datetime('at')->apply(new Update(filters: []))->filters);
        $this->assertSame(['point', 'encrypt'], Attribute::point('at')->apply(new Update(filters: ['encrypt']))->filters);
    }

    public function testFormatReplacesTheFormat(): void
    {
        $updated = Attribute::string('name', format: new Format('email'))->apply(new Update(format: new Format('url', ['scheme' => 'https'])));

        $this->assertSame('url', $updated->format?->name);
        $this->assertSame(['scheme' => 'https'], $updated->format->options);
    }

    public function testNullFormatIsAChange(): void
    {
        $update = new Update(format: null);

        $this->assertTrue($update->changesFormat());
        $this->assertFalse($update->isEmpty());
    }

    public function testNullFormatRemovesTheFormat(): void
    {
        $updated = Attribute::string('name', format: new Format('email', ['strict' => true]))->apply(new Update(format: null));

        $this->assertNull($updated->format);
        $this->assertNull($updated->toDocument()->getAttribute('format'));
        $this->assertSame([], $updated->toDocument()->getAttribute('formatOptions'));
    }

    public function testOmittedFormatKeepsTheFormat(): void
    {
        $format = new Format('email', ['strict' => true]);

        $updated = Attribute::string('name', format: $format)->apply(new Update(size: 32));

        $this->assertSame($format, $updated->format);
    }

    public function testSignedAndArrayChange(): void
    {
        $updated = Attribute::integer('count')->apply(new Update(signed: false, array: true));

        $this->assertFalse($updated->signed);
        $this->assertTrue($updated->array);
    }

    public function testTypeChangesToAnotherAttributeType(): void
    {
        $updated = Attribute::string('body', 100)->apply(new Update(type: ColumnType::Text));

        $this->assertSame(ColumnType::Text, $updated->type);
        $this->assertSame('text', $updated->toDocument()->getAttribute('type'));
        $this->assertSame(100, $updated->size);
    }

    /**
     * @return array<string, array{Attribute, ColumnType, Attribute}>
     */
    public static function typeChanges(): array
    {
        $string = Attribute::string('field', 64);
        $integer = Attribute::integer('field', signed: false, array: true, width: IntegerWidth::Bits64);
        $datetime = Attribute::datetime('field', array: true);
        $point = Attribute::point('field');

        return [
            'string to varchar' => [$string, ColumnType::Varchar, Attribute::varchar('field', 64)],
            'string to text' => [$string, ColumnType::Text, Attribute::text('field', 64)],
            'string to mediumText' => [$string, ColumnType::MediumText, Attribute::mediumText('field', 64)],
            'string to longText' => [$string, ColumnType::LongText, Attribute::longText('field', 64)],
            'string to integer' => [$string, ColumnType::Integer, Attribute::integer('field', width: IntegerWidth::Bits64)],
            'string to bigInteger' => [$string, ColumnType::BigInteger, Attribute::bigInteger('field')],
            'string to float' => [$string, ColumnType::Float, Attribute::float('field')],
            'string to double' => [$string, ColumnType::Double, Attribute::double('field')],
            'string to boolean' => [$string, ColumnType::Boolean, Attribute::boolean('field')],
            'string to datetime' => [$string, ColumnType::Datetime, Attribute::datetime('field')],
            'string to id' => [$string, ColumnType::Id, Attribute::id('field')],
            'string to point' => [$string, ColumnType::Point, Attribute::point('field')],
            'string to lineString' => [$string, ColumnType::Linestring, Attribute::lineString('field')],
            'string to polygon' => [$string, ColumnType::Polygon, Attribute::polygon('field')],
            'string to object' => [$string, ColumnType::Object, Attribute::object('field')],
            'string to vector' => [$string, ColumnType::Vector, Attribute::vector('field', 64)],
            'unsigned integer to string' => [$integer, ColumnType::String, Attribute::string('field', 8, array: true)],
            'unsigned integer to integer of another width' => [Attribute::integer('field', signed: false), ColumnType::BigInteger, Attribute::bigInteger('field', signed: false)],
            'unsigned integer to double' => [$integer, ColumnType::Double, Attribute::double('field', signed: false, array: true)],
            'unsigned integer to boolean' => [$integer, ColumnType::Boolean, Attribute::boolean('field', array: true)],
            'unsigned integer to id' => [$integer, ColumnType::Id, Attribute::id('field', array: true)],
            'unsigned integer to datetime' => [$integer, ColumnType::Datetime, Attribute::datetime('field', array: true)],
            'integer array to point' => [$integer, ColumnType::Point, Attribute::point('field')],
            'integer array to vector' => [$integer, ColumnType::Vector, Attribute::vector('field', 8)],
            'datetime to integer' => [$datetime, ColumnType::Integer, Attribute::integer('field', signed: false, array: true)],
            'datetime to string' => [$datetime, ColumnType::String, Attribute::string('field', 0, array: true)],
            'point to text' => [$point, ColumnType::Text, Attribute::text('field')],
            'point to polygon' => [$point, ColumnType::Polygon, Attribute::polygon('field')],
        ];
    }

    #[DataProvider('typeChanges')]
    public function testTypeChangeProducesWhatTheTargetFactoryProduces(Attribute $source, ColumnType $type, Attribute $expected): void
    {
        $updated = $source->apply(new Update(type: $type));

        $this->assertSame($expected->toDocument()->getArrayCopy(), $updated->toDocument()->getArrayCopy());
    }

    public function testTypeChangeKeepsUserFiltersAndSwapsTheTypeFilter(): void
    {
        $updated = Attribute::string('field', filters: ['encrypt'])
            ->apply(new Update(type: ColumnType::Datetime))
            ->apply(new Update(type: ColumnType::Polygon));

        $this->assertSame(['polygon', 'encrypt'], $updated->filters);
    }

    public function testExplicitFiltersOnATypeChangeAreKept(): void
    {
        $updated = Attribute::datetime('field')->apply(new Update(type: ColumnType::String, filters: ['datetime']));

        $this->assertSame(['datetime'], $updated->filters);
    }

    public function testUpdatesCannotBreakTheTypeInvariants(): void
    {
        $this->assertTrue(Attribute::string('name')->apply(new Update(signed: false))->signed);
        $this->assertFalse(Attribute::datetime('at')->apply(new Update(signed: true))->signed);
        $this->assertFalse(Attribute::point('at')->apply(new Update(array: true))->array);
        $this->assertNull(Attribute::boolean('flag')->apply(new Update(size: 4))->size);
        $this->assertSame(8, Attribute::integer('count')->apply(new Update(size: 20))->size);
        $this->assertNull(Attribute::integer('count', width: IntegerWidth::Bits64)->apply(new Update(size: 4))->size);
    }

    public function testTypeOutsideTheAttributeTypesIsRejected(): void
    {
        $this->expectException(Structure::class);

        Attribute::string('body')->apply(new Update(type: ColumnType::Tuple));
    }

    public function testTypeCannotBecomeRelationship(): void
    {
        $this->expectException(Structure::class);

        Attribute::string('author')->apply(new Update(type: ColumnType::Relationship));
    }

    public function testSameTypeIsAccepted(): void
    {
        $this->assertSame(ColumnType::Integer, Attribute::integer('count')->apply(new Update(type: ColumnType::Integer))->type);
    }

    public function testRelationshipAttributeCannotChangeType(): void
    {
        $attribute = Attribute::relationship('author', Relationship::manyToOne('users'), RelationshipSide::Parent);

        $this->expectException(Structure::class);

        $attribute->apply(new Update(type: ColumnType::String));
    }

    public function testRenamingARelationshipAttributeRenamesItsRelationship(): void
    {
        $attribute = Attribute::relationship('author', Relationship::manyToOne('users'), RelationshipSide::Parent);

        $renamed = $attribute->apply(new Update(key: 'writer'));

        $this->assertSame('writer', $renamed->key);
        $this->assertSame('writer', $renamed->relationship?->key);
        $this->assertSame('users', $renamed->relationship->relatedCollection);
        $this->assertSame(RelationshipSide::Parent, $renamed->side);
    }
}
