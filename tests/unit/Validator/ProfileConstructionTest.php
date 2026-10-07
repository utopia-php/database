<?php

namespace Tests\Unit\Validator;

use DateTime;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\Profiles;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\AttributeDefinition;
use Utopia\Database\Validator\IndexDefinition;
use Utopia\Database\Validator\Queries\Document as DocumentValidator;
use Utopia\Database\Validator\Queries\Documents;
use Utopia\Database\Validator\Queries\Narrow;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;

final class ProfileConstructionTest extends TestCase
{
    public function testAnAttributeDefinitionTakesItsSizeCapsFromTheLimits(): void
    {
        $validator = new AttributeDefinition([], Profiles::of(string: 100, varchar: 50, integer: 1000));

        $this->assertTrue($validator->isValid(Attribute::string(key: 'title', size: 100)));
        $this->assertRefused('Max size allowed for string is: 100', static fn (): bool => $validator->isValid(Attribute::string(key: 'title', size: 101)));
        $this->assertRefused('Max size allowed for varchar is: 50', static fn (): bool => $validator->isValid(Attribute::varchar(key: 'code', size: 51)));
    }

    public function testAnAttributeDefinitionCountsColumnsAndWidthAgainstTheLimits(): void
    {
        $profile = Profiles::of(string: 1000, attributes: 3, documentSize: 100);
        $attribute = Attribute::string(key: 'title', size: 10);

        $withinLimits = new AttributeDefinition([], $profile, attributeCount: static fn (Document $attribute): int => 3, attributeWidth: static fn (Document $attribute): int => 99);
        $this->assertTrue($withinLimits->isValid($attribute));

        $tooMany = new AttributeDefinition([], $profile, attributeCount: static fn (Document $attribute): int => 4, attributeWidth: static fn (Document $attribute): int => 0);
        $this->expectException(LimitException::class);
        $this->expectExceptionMessage('Current attribute count is 4 but the maximum is 3');
        $tooMany->isValid($attribute);
    }

    public function testAnAttributeDefinitionAcceptsTheTypesTheProfileOffers(): void
    {
        $bare = new AttributeDefinition([], Profiles::of());
        $this->assertRefused('Vector types are not supported by the current database', static fn (): bool => $bare->isValid(Attribute::vector(key: 'embedding', dimensions: 3)));
        $this->assertRefused('Spatial attributes are not supported', static fn (): bool => $bare->isValid(Attribute::point(key: 'location')));
        $this->assertRefused('Object attributes are not supported', static fn (): bool => $bare->isValid(Attribute::object(key: 'meta')));

        $capable = new AttributeDefinition([], Profiles::of(capabilities: [Capability::Vectors, Capability::Objects], features: [Feature\Spatial::class]));
        $this->assertTrue($capable->isValid(Attribute::vector(key: 'embedding', dimensions: 3)));
        $this->assertTrue($capable->isValid(Attribute::point(key: 'location')));
        $this->assertTrue($capable->isValid(Attribute::object(key: 'meta')));
    }

    public function testTheAvailableTypesFollowTheProfile(): void
    {
        $bare = Attribute::availableTypes(Profiles::of());
        $this->assertNotContains(ColumnType::Object, $bare);
        $this->assertNotContains(ColumnType::Point, $bare);
        $this->assertNotContains(ColumnType::Linestring, $bare);
        $this->assertNotContains(ColumnType::Polygon, $bare);
        $this->assertNotContains(ColumnType::Vector, $bare);
        $this->assertContains(ColumnType::String, $bare);
        $this->assertContains(ColumnType::Relationship, $bare);

        $full = Attribute::availableTypes(Profiles::of(capabilities: [Capability::Vectors, Capability::Objects], features: [Feature\Spatial::class]));
        $this->assertSame(Attribute::TYPES, $full);

        $spatialOnly = Attribute::availableTypes(Profiles::of(features: [Feature\Spatial::class]));
        $this->assertSame(
            [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon],
            \array_values(\array_filter($spatialOnly, static fn (ColumnType $type): bool => ! \in_array($type, $bare, true))),
        );
    }

    public function testAnIndexDefinitionTakesItsLengthAndReservedKeysFromTheLimits(): void
    {
        $attributes = [Attribute::string(key: 'title', size: 100)];
        $profile = Profiles::of(capabilities: [Capability::DefinedAttributes, Capability::IndexKey], indexLength: 99, internalIndexKeys: ['primary']);

        $validator = new IndexDefinition($attributes, [], $profile);
        $this->assertFalse($validator->isValid(Index::key(key: 'title_key', attributes: ['title'])));
        $this->assertSame('Index length is longer than the maximum: 99', $validator->getDescription());

        $validator = new IndexDefinition($attributes, [], $profile);
        $this->assertFalse($validator->isValid(Index::key(key: 'primary', attributes: ['title'], lengths: [10])));
        $this->assertSame('Index key name is reserved', $validator->getDescription());
    }

    public function testAnIndexDefinitionAcceptsOnlyTheIndexTypesTheProfileSupports(): void
    {
        $attributes = [Attribute::string(key: 'title', size: 100)];
        $index = Index::key(key: 'title_key', attributes: ['title']);

        $without = new IndexDefinition($attributes, [], Profiles::of(capabilities: [Capability::DefinedAttributes], indexLength: 768));
        $this->assertFalse($without->isValid($index));
        $this->assertSame('Key index is not supported', $without->getDescription());

        $with = new IndexDefinition($attributes, [], Profiles::of(capabilities: [Capability::DefinedAttributes, Capability::IndexKey], indexLength: 768));
        $this->assertTrue($with->isValid($index));
    }

    public function testADocumentsValidatorTakesItsGrammarFromTheProfile(): void
    {
        $attributes = [Attribute::string(key: 'title', size: 64)];
        $count = [Query::count('*', 'rows')];
        $join = [Query::join('other', 'o', [Query::on('$id', 'o.$id')])];

        $plain = new Documents($attributes, [], Profiles::of(capabilities: [Capability::DefinedAttributes]));
        $this->assertFalse($plain->isValid($count));
        $this->assertSame('Invalid query method: count', $plain->getDescription());
        $this->assertFalse($plain->isValid($join));

        $grammar = new Documents($attributes, [], Profiles::of(capabilities: [Capability::DefinedAttributes, Capability::Aggregations, Capability::Joins]));
        $this->assertTrue($grammar->isValid($count));
    }

    public function testQueryValidatorsTakeTheUidLengthFromTheLimits(): void
    {
        $attributes = [Attribute::string(key: 'title', size: 64)];
        $cursor = [Query::cursorAfter(new Document(['$id' => \str_repeat('a', 40)]))];

        $this->assertFalse(new Documents($attributes, [], Profiles::of(uidLength: 36))->isValid($cursor));
        $this->assertTrue(new Documents($attributes, [], Profiles::of(uidLength: 255))->isValid($cursor));
        $this->assertFalse(Narrow::of($cursor, $attributes, Profiles::of(uidLength: 36), 5000)?->isValid($cursor) ?? true);
        $this->assertTrue(Narrow::of($cursor, $attributes, Profiles::of(uidLength: 255), 5000)?->isValid($cursor) ?? false);
    }

    public function testADocumentValidatorAcceptsTheTenantOnlyUnderSharedTables(): void
    {
        $select = [Query::select(['$tenant'])];

        $this->assertFalse(new DocumentValidator([], Profiles::of(capabilities: [Capability::DefinedAttributes]))->isValid($select));
        $this->assertTrue(new DocumentValidator([], Profiles::of(capabilities: [Capability::DefinedAttributes], sharedTables: true))->isValid($select));
    }

    public function testAStructureTakesItsDatetimeRangeFromTheLimits(): void
    {
        $collection = Collection::create(id: 'events', attributes: [Attribute::datetime(key: 'at')]);
        $collection->setAttribute('$collection', Database::METADATA);
        $profile = Profiles::of(capabilities: [Capability::DefinedAttributes], minDateTime: new DateTime('2000-01-01'), maxDateTime: new DateTime('2100-01-01'));

        $this->assertTrue(new Structure($collection, $profile)->isValid($this->event('2050-06-01 00:00:00')));

        $structure = new Structure($collection, $profile);
        $this->assertFalse($structure->isValid($this->event('1999-12-31 00:00:00')));
        $this->assertStringContainsString('2000-01-01', $structure->getDescription());
    }

    private function event(string $at): Document
    {
        return new Document([
            '$id' => 'event',
            '$collection' => 'events',
            '$permissions' => [],
            '$createdAt' => '2050-01-01 00:00:00',
            '$updatedAt' => '2050-01-01 00:00:00',
            'at' => $at,
        ]);
    }

    /**
     * @param  callable(): bool  $validation
     */
    private function assertRefused(string $message, callable $validation): void
    {
        try {
            $validation();
            $this->fail('Expected the validator to refuse with: '.$message);
        } catch (DatabaseException $exception) {
            $this->assertSame($message, $exception->getMessage());
        }
    }
}
