<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\Profile;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\AttributeDefinition;
use Utopia\Database\Validator\IndexDefinition;
use Utopia\Database\Validator\Queries\Documents;

final class ProfileTest extends TestCase
{
    public function testAProfileAnswersForTheCapabilitiesAndFeaturesItWasBuiltWith(): void
    {
        $profile = new Profile(
            (new Memory())->limits(),
            [Capability::Joins, Capability::IndexKey],
            [Feature\Spatial::class],
            true,
            false,
        );

        $this->assertTrue($profile->supports(Capability::Joins));
        $this->assertTrue($profile->supports(Capability::IndexKey));
        $this->assertFalse($profile->supports(Capability::Aggregations));
        $this->assertTrue($profile->hasFeature(Feature\Spatial::class));
        $this->assertFalse($profile->hasFeature(Feature\Upserts::class));
        $this->assertTrue($profile->sharedTables);
        $this->assertFalse($profile->migrating);
    }

    public function testTheDatabaseProfileIsWhatItsAdapterSupports(): void
    {
        $adapter = new MariaDB(new stdClass());
        $profile = $this->database($adapter)->profile();

        foreach (Capability::cases() as $capability) {
            $this->assertSame($adapter->supports($capability), $profile->supports($capability), $capability->name);
        }
        $this->assertTrue($profile->hasFeature(Feature\Spatial::class));
        $this->assertTrue($profile->hasFeature(Feature\Upserts::class));
        $this->assertFalse($profile->hasFeature(Feature\Schemaless::class));
        $this->assertSame($adapter->limits(), $profile->limits);
        $this->assertFalse($profile->sharedTables);
        $this->assertFalse($profile->migrating);
    }

    public function testSharedTablesShrinkTheIndexKeyOnlyAfterTheyAreTurnedOn(): void
    {
        $database = $this->database(new MariaDB(new stdClass()));
        $attributes = [Attribute::string(key: 'title', size: 768)];
        $index = Index::key(key: 'title_key', attributes: ['title']);

        $this->assertFalse($database->profile()->sharedTables);
        $this->assertTrue(new IndexDefinition($attributes, [], $database->profile())->isValid($index));

        $database->setSharedTables(true);

        $this->assertTrue($database->profile()->sharedTables);
        $this->assertSame(767, $database->profile()->limits->indexLength);
        $validator = new IndexDefinition($attributes, [], $database->profile());
        $this->assertFalse($validator->isValid($index));
        $this->assertSame('Index length is longer than the maximum: 767', $validator->getDescription());

        $database->setSharedTables(false);

        $this->assertTrue(new IndexDefinition($attributes, [], $database->profile())->isValid($index));
    }

    public function testTheTenantIsSelectableOnlyAfterSharedTablesAreTurnedOn(): void
    {
        $database = $this->database(new Memory());
        $select = [Query::select(['$tenant'])];

        $this->assertFalse(new Documents([], [], $database->profile())->isValid($select));

        $database->setSharedTables(true);

        $this->assertTrue(new Documents([], [], $database->profile())->isValid($select));
    }

    public function testASchemaColumnClashesOnlyUntilASharedTableMigrationStarts(): void
    {
        $database = $this->database(new MariaDB(new stdClass()))->setSharedTables(true);
        $schema = [Attribute::string(key: 'orphan', size: 16)];
        $attribute = Attribute::string(key: 'orphan', size: 16);

        $this->assertFalse($database->profile()->migrating);
        try {
            (new AttributeDefinition([], $database->profile(), $schema))->isValid($attribute);
            $this->fail('A column the schema already holds must clash outside a migration');
        } catch (DuplicateException $exception) {
            $this->assertSame('Attribute already exists in schema', $exception->getMessage());
        }

        $database->setMigrating(true);

        $this->assertTrue($database->profile()->migrating);
        $this->assertTrue((new AttributeDefinition([], $database->profile(), $schema))->isValid($attribute));

        $database->setMigrating(false);

        $this->assertFalse($database->profile()->migrating);
        $this->expectException(DuplicateException::class);
        (new AttributeDefinition([], $database->profile(), $schema))->isValid($attribute);
    }

    public function testUndeclaredAttributesAreQueryableOnlyAfterTheSchemalessModeIsOn(): void
    {
        $adapter = $this->mongo();
        $database = $this->database($adapter);
        $filter = [Query::equal('undeclared', ['value'])];

        $this->assertTrue($database->profile()->supports(Capability::DefinedAttributes));
        $this->assertFalse(new Documents([], [], $database->profile())->isValid($filter));

        $this->assertSame($database, $database->setSchemaless(true));

        $this->assertTrue($adapter->isSchemaless());
        $this->assertFalse($database->profile()->supports(Capability::DefinedAttributes));
        $this->assertTrue(new Documents([], [], $database->profile())->isValid($filter));

        $database->setSchemaless(false);

        $this->assertFalse($adapter->isSchemaless());
        $this->assertTrue($database->profile()->supports(Capability::DefinedAttributes));
        $this->assertFalse(new Documents([], [], $database->profile())->isValid($filter));
    }

    public function testAnAdapterWithoutASchemalessModeOnlyKeepsItsSchema(): void
    {
        $database = $this->database(new Memory());

        $this->assertSame($database, $database->setSchemaless(false));
        $this->assertTrue($database->profile()->supports(Capability::DefinedAttributes));

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support schemaless');
        $database->setSchemaless(true);
    }

    private function database(Adapter $adapter): Database
    {
        return new Database($adapter, new Cache(new None()));
    }

    private function mongo(): Mongo
    {
        return new class () extends Mongo {
            public function __construct()
            {
            }
        };
    }
}
