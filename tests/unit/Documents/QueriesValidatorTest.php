<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;

final class QueriesValidatorTest extends TestCase
{
    private const string COLLECTION = 'books';

    public function testANarrowReadBuildsNoDocumentsValidator(): void
    {
        $database = $this->database();

        $this->assertSame(1, $database->count(self::COLLECTION, [Query::equal('title', ['Dune'])]));
        $this->assertSame(6, $database->sum(self::COLLECTION, 'year', [Query::greaterThan('year', 1)]));
        $this->assertCount(1, $database->find(self::COLLECTION, [Query::equal('title', ['Dune']), Query::orderDesc('year'), Query::limit(5), Query::offset(0)]));
        $this->assertSame(0, $database->documentsValidators);
    }

    public function testAnyOtherReadStillUsesTheDocumentsValidator(): void
    {
        $database = $this->database();
        $database->createIndex(self::COLLECTION, Index::fulltext('title_fulltext', ['title']));

        $database->count(self::COLLECTION, [Query::search('title', 'Dune')]);
        $database->find(self::COLLECTION, [Query::select(['title']), Query::equal('title', ['Dune'])]);

        $this->assertSame(2, $database->documentsValidators);
    }

    public function testANarrowReadFollowsEverySchemaChange(): void
    {
        $database = $this->database();
        $byYear = [Query::equal('year', [6])];

        $this->assertSame(1, $database->count(self::COLLECTION, $byYear));

        $database->updateAttribute(self::COLLECTION, 'year', new AttributeUpdate(type: ColumnType::String, size: 8));
        $this->assertRefused($database, $byYear, 'Invalid query: Query value is invalid for attribute "year"');
        $this->assertSame(0, $database->count(self::COLLECTION, [Query::equal('year', ['six'])]));

        $database->renameAttribute(self::COLLECTION, 'year', 'published');
        $this->assertRefused($database, [Query::equal('year', ['six'])], 'Invalid query: Attribute not found in schema: year');
        $this->assertSame(0, $database->count(self::COLLECTION, [Query::equal('published', ['six'])]));

        $database->updateAttribute(self::COLLECTION, 'published', new AttributeUpdate(filters: ['encrypt']));
        $this->assertRefused($database, [Query::equal('published', ['six'])], 'Invalid query: Cannot query encrypted attribute: published');

        $database->deleteAttribute(self::COLLECTION, 'published');
        $this->assertRefused($database, [Query::equal('published', ['six'])], 'Invalid query: Attribute not found in schema: published');

        $database->createAttribute(self::COLLECTION, Attribute::integer(key: 'published'));
        $this->assertSame(0, $database->count(self::COLLECTION, [Query::equal('published', [6])]));
        $this->assertRefused($database, [Query::equal('published', ['six'])], 'Invalid query: Query value is invalid for attribute "published"');
    }

    public function testAnIndexChangeReachesTheListsThatNeedAnIndex(): void
    {
        $database = $this->database();
        $search = [Query::search('title', 'Dune')];

        $this->assertRefused($database, $search, 'Searching by attribute "title" requires a fulltext index.');

        $database->createIndex(self::COLLECTION, Index::fulltext('title_fulltext', ['title']));
        $database->count(self::COLLECTION, $search);

        $database->deleteIndex(self::COLLECTION, 'title_fulltext');
        $this->assertRefused($database, $search, 'Searching by attribute "title" requires a fulltext index.');
    }

    public function testACollectionChangedInPlaceIsJudgedAsItIsNow(): void
    {
        $database = $this->database();
        $collection = $database->getCollection(self::COLLECTION);
        $queries = [Query::equal('title', ['Dune'])];

        $this->assertTrue($database->queriesValidator($collection, $queries)->isValid($queries));

        $attributes = $collection->getAttribute('attributes', []);
        $this->assertIsArray($attributes);
        $title = $attributes[0];
        $this->assertInstanceOf(Document::class, $title);
        $title->setAttribute('type', ColumnType::Integer->value);
        $collection->setAttribute('attributes', $attributes);
        $validator = $database->queriesValidator($collection, $queries);
        $this->assertFalse($validator->isValid($queries));
        $this->assertSame('Invalid query: Query value is invalid for attribute "title"', $validator->getDescription());

        $collection->setAttribute('attributes', []);
        $validator = $database->queriesValidator($collection, $queries);
        $this->assertFalse($validator->isValid($queries));
        $this->assertSame('Invalid query: Attribute not found in schema: title', $validator->getDescription());

        $collection->setAttribute('attributes', [Attribute::string(key: 'title', size: 64)->toDocument()]);
        $this->assertTrue($database->queriesValidator($collection, $queries)->isValid($queries));
    }

    public function testACollectionALifecycleHookChangedIsJudgedAsItWasHanded(): void
    {
        $database = $this->database();
        $database->addHook(new class () implements Lifecycle {
            public function handle(Domain $event): void
            {
                if ($event instanceof Event\Collection\Read) {
                    $event->definition->setAttribute('attributes', [Attribute::integer(key: 'title')->toDocument()]);
                }
            }
        });
        $queries = [Query::equal('title', ['Dune'])];

        $changed = $database->getCollection(self::COLLECTION);
        $validator = $database->queriesValidator($changed, $queries);
        $this->assertFalse($validator->isValid($queries));
        $this->assertSame('Invalid query: Query value is invalid for attribute "title"', $validator->getDescription());

        $this->assertTrue($database->queriesValidator($database->silent(fn () => $database->getCollection(self::COLLECTION)), $queries)->isValid($queries));
        $this->assertSame(1, $database->count(self::COLLECTION, $queries));
    }

    public function testEachTenantIsJudgedByItsOwnCollection(): void
    {
        $database = $this->database(sharedTables: true);
        $queries = [Query::equal('year', [6])];

        $this->assertSame(1, $database->count(self::COLLECTION, $queries));

        $database->setTenant(2);
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64), Attribute::string(key: 'year', size: 8)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $this->assertRefused($database, $queries, 'Invalid query: Query value is invalid for attribute "year"');

        $database->setTenant(1);
        $this->assertSame(1, $database->count(self::COLLECTION, $queries));
    }

    /**
     * @param  array<Query>  $queries
     */
    private function assertRefused(Database $database, array $queries, string $message): void
    {
        foreach (['count', 'find'] as $read) {
            try {
                $read === 'count' ? $database->count(self::COLLECTION, $queries) : $database->find(self::COLLECTION, $queries);
                $this->fail($read.' accepted a list the schema refuses');
            } catch (QueryException $exception) {
                $this->assertSame($message, $exception->getMessage(), $read);
            }
        }
    }

    private function database(bool $sharedTables = false): DocumentsValidatorDatabase
    {
        $database = new DocumentsValidatorDatabase(new Memory(), new Cache(new MemoryCache()));
        $database->setDatabase('queries_validator')->setNamespace('queries_validator_'.\uniqid());
        if ($sharedTables) {
            $database->setSharedTables(true)->setTenant(1);
        }
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64), Attribute::integer(key: 'year')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'dune', 'title' => 'Dune', 'year' => 6]));
        $database->documentsValidators = 0;

        return $database;
    }
}
