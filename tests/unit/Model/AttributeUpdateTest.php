<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Exception\Structure;
use Utopia\Database\Filter;
use Utopia\Database\Format;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\Unchanged;
use Utopia\Query\Schema\ColumnType;

final class AttributeUpdateTest extends TestCase
{
    public function testDefaultsLeaveEverythingUnchanged(): void
    {
        $update = new AttributeUpdate();

        $this->assertTrue($update->isEmpty());
        $this->assertFalse($update->changesDefault());
        $this->assertSame(Unchanged::Value, $update->default);
    }

    public function testNullDefaultIsAChange(): void
    {
        $update = new AttributeUpdate(default: null);

        $this->assertTrue($update->changesDefault());
        $this->assertFalse($update->isEmpty());
    }

    public function testAnySingleFieldMakesTheUpdateNonEmpty(): void
    {
        $updates = [
            new AttributeUpdate(type: ColumnType::Text),
            new AttributeUpdate(size: 10),
            new AttributeUpdate(required: false),
            new AttributeUpdate(default: 'x'),
            new AttributeUpdate(signed: false),
            new AttributeUpdate(array: false),
            new AttributeUpdate(format: new Format('email')),
            new AttributeUpdate(filters: []),
            new AttributeUpdate(key: 'renamed'),
        ];

        foreach ($updates as $update) {
            $this->assertFalse($update->isEmpty());
        }
    }

    public function testEmptyUpdateKeepsEveryField(): void
    {
        $original = Attribute::string('name', 64, required: true, default: 'x', array: true, format: new Format('email', ['a' => 1]), filters: ['lowercase']);

        $updated = $original->apply(new AttributeUpdate());

        $this->assertSame($original->toDocument()->getArrayCopy(), $updated->toDocument()->getArrayCopy());
    }

    public function testApplyChangesOnlyTheGivenFields(): void
    {
        $original = Attribute::string('name', 64, required: true, default: 'x', filters: ['lowercase']);

        $updated = $original->apply(new AttributeUpdate(size: 128, required: false));

        $this->assertSame(128, $updated->size);
        $this->assertFalse($updated->required);
        $this->assertSame('x', $updated->default);
        $this->assertSame(['lowercase'], $updated->filters);
        $this->assertSame('name', $updated->key);
        $this->assertSame(ColumnType::String, $updated->type);
    }

    public function testZeroSizeClearsTheSize(): void
    {
        $this->assertNull(Attribute::text('body', 500)->apply(new AttributeUpdate(size: 0))->size);
    }

    public function testApplyLeavesTheOriginalUntouched(): void
    {
        $original = Attribute::string('name', 64);

        $original->apply(new AttributeUpdate(size: 128, key: 'title', default: 'y'));

        $this->assertSame(64, $original->size);
        $this->assertSame('name', $original->key);
        $this->assertNull($original->default);
    }

    public function testNullDefaultClearsTheDefault(): void
    {
        $updated = Attribute::integer('count', default: 5)->apply(new AttributeUpdate(default: null));

        $this->assertNull($updated->default);
    }

    public function testDefaultCanBeSet(): void
    {
        $updated = Attribute::integer('count')->apply(new AttributeUpdate(default: 7));

        $this->assertSame(7, $updated->default);
    }

    public function testKeyRenames(): void
    {
        $this->assertSame('title', Attribute::string('name')->apply(new AttributeUpdate(key: 'title'))->key);
    }

    public function testFiltersAcceptEnumCasesAndStrings(): void
    {
        $updated = Attribute::string('name')->apply(new AttributeUpdate(filters: [Filter::Json, 'encrypt']));

        $this->assertSame(['json', 'encrypt'], $updated->filters);
    }

    public function testEmptyFiltersClearTheFilters(): void
    {
        $this->assertSame([], Attribute::datetime('at')->apply(new AttributeUpdate(filters: []))->filters);
    }

    public function testFormatReplacesTheFormat(): void
    {
        $updated = Attribute::string('name', format: new Format('email'))->apply(new AttributeUpdate(format: new Format('url', ['scheme' => 'https'])));

        $this->assertSame('url', $updated->format?->name);
        $this->assertSame(['scheme' => 'https'], $updated->format?->options);
    }

    public function testSignedAndArrayChange(): void
    {
        $updated = Attribute::integer('count')->apply(new AttributeUpdate(signed: false, array: true));

        $this->assertFalse($updated->signed);
        $this->assertTrue($updated->array);
    }

    public function testTypeChangesToAnotherAttributeType(): void
    {
        $updated = Attribute::string('body', 100)->apply(new AttributeUpdate(type: ColumnType::Text));

        $this->assertSame(ColumnType::Text, $updated->type);
        $this->assertSame('text', $updated->toDocument()->getAttribute('type'));
        $this->assertSame(100, $updated->size);
    }

    public function testTypeOutsideTheAttributeTypesIsRejected(): void
    {
        $this->expectException(Structure::class);

        Attribute::string('body')->apply(new AttributeUpdate(type: ColumnType::Tuple));
    }

    public function testTypeCannotBecomeRelationship(): void
    {
        $this->expectException(Structure::class);

        Attribute::string('author')->apply(new AttributeUpdate(type: ColumnType::Relationship));
    }

    public function testSameTypeIsAccepted(): void
    {
        $this->assertSame(ColumnType::Integer, Attribute::integer('count')->apply(new AttributeUpdate(type: ColumnType::Integer))->type);
    }

    public function testRelationshipAttributeCannotChangeType(): void
    {
        $attribute = Attribute::relationship('author', Relationship::manyToOne('users'), RelationshipSide::Parent);

        $this->expectException(Structure::class);

        $attribute->apply(new AttributeUpdate(type: ColumnType::String));
    }

    public function testRenamingARelationshipAttributeRenamesItsRelationship(): void
    {
        $attribute = Attribute::relationship('author', Relationship::manyToOne('users'), RelationshipSide::Parent);

        $renamed = $attribute->apply(new AttributeUpdate(key: 'writer'));

        $this->assertSame('writer', $renamed->key);
        $this->assertSame('writer', $renamed->relationship?->key);
        $this->assertSame('users', $renamed->relationship?->relatedCollection);
        $this->assertSame(RelationshipSide::Parent, $renamed->side);
    }
}
