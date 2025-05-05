<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

trait GeneralTests
{
    public function testPing(): void
    {
        $this->assertEquals(true, static::getDatabase()->ping());
    }

    /**
     * @throws AuthorizationException
     * @throws DuplicateException
     * @throws ConflictException
     * @throws LimitException
     * @throws StructureException
     * @throws DatabaseException
     */
    public function testQueryTimeout(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForTimeouts()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('global-timeouts');

        $this->assertEquals(
            true,
            static::getDatabase()->createAttribute(
                collection: 'global-timeouts',
                id: 'longtext',
                type: Database::VAR_STRING,
                size: 100000000,
                required: true
            )
        );

        for ($i = 0; $i < 20; $i++) {
            static::getDatabase()->createDocument('global-timeouts', new Document([
                'longtext' => file_get_contents(__DIR__ . '/../../../resources/longtext.txt'),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any())
                ]
            ]));
        }

        static::getDatabase()->setTimeout(1);

        try {
            static::getDatabase()->find('global-timeouts', [
                Query::notEqual('longtext', 'appwrite'),
            ]);
            $this->fail('Failed to throw exception');
        } catch (\Exception $e) {
            static::getDatabase()->clearTimeout();
            static::getDatabase()->deleteCollection('global-timeouts');
            $this->assertInstanceOf(TimeoutException::class, $e);
        }
    }


    public function testCreatedAtUpdatedAt(): void
    {
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('created_at'));
        static::getDatabase()->createAttribute('created_at', 'title', Database::VAR_STRING, 100, false);
        $document = static::getDatabase()->createDocument('created_at', new Document([
            '$id' => ID::custom('uid123'),

            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]));

        $this->assertNotEmpty($document->getInternalId());
        $this->assertNotNull($document->getInternalId());
    }


    public function testPreserveDatesUpdate(): void
    {
        Authorization::disable();

        static::getDatabase()->setPreserveDates(true);

        static::getDatabase()->createCollection('preserve_update_dates');

        static::getDatabase()->createAttribute('preserve_update_dates', 'attr1', Database::VAR_STRING, 10, false);

        $doc1 = static::getDatabase()->createDocument('preserve_update_dates', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
            'attr1' => 'value1',
        ]));

        $doc2 = static::getDatabase()->createDocument('preserve_update_dates', new Document([
            '$id' => 'doc2',
            '$permissions' => [],
            'attr1' => 'value2',
        ]));

        $doc3 = static::getDatabase()->createDocument('preserve_update_dates', new Document([
            '$id' => 'doc3',
            '$permissions' => [],
            'attr1' => 'value3',
        ]));

        $newDate = '2000-01-01T10:00:00.000+00:00';

        $doc1->setAttribute('$updatedAt', $newDate);
        $doc1 = static::getDatabase()->updateDocument('preserve_update_dates', 'doc1', $doc1);
        $this->assertEquals($newDate, $doc1->getAttribute('$updatedAt'));
        $doc1 = static::getDatabase()->getDocument('preserve_update_dates', 'doc1');
        $this->assertEquals($newDate, $doc1->getAttribute('$updatedAt'));

        $this->getDatabase()->updateDocuments(
            'preserve_update_dates',
            new Document([
                '$updatedAt' => $newDate
            ]),
            [
                Query::equal('$id', [
                    $doc2->getId(),
                    $doc3->getId()
                ])
            ]
        );

        $doc2 = static::getDatabase()->getDocument('preserve_update_dates', 'doc2');
        $doc3 = static::getDatabase()->getDocument('preserve_update_dates', 'doc3');
        $this->assertEquals($newDate, $doc2->getAttribute('$updatedAt'));
        $this->assertEquals($newDate, $doc3->getAttribute('$updatedAt'));

        static::getDatabase()->deleteCollection('preserve_update_dates');

        static::getDatabase()->setPreserveDates(false);

        Authorization::reset();
    }

    public function testPreserveDatesCreate(): void
    {
        Authorization::disable();

        static::getDatabase()->setPreserveDates(true);

        static::getDatabase()->createCollection('preserve_create_dates');

        static::getDatabase()->createAttribute('preserve_create_dates', 'attr1', Database::VAR_STRING, 10, false);

        $date = '2000-01-01T10:00:00.000+00:00';

        static::getDatabase()->createDocument('preserve_create_dates', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
            'attr1' => 'value1',
            '$createdAt' => $date
        ]));

        static::getDatabase()->createDocuments('preserve_create_dates', [
            new Document([
                '$id' => 'doc2',
                '$permissions' => [],
                'attr1' => 'value2',
                '$createdAt' => $date
            ]),
            new Document([
                '$id' => 'doc3',
                '$permissions' => [],
                'attr1' => 'value3',
                '$createdAt' => $date
            ]),
        ], batchSize: 2);

        $doc1 = static::getDatabase()->getDocument('preserve_create_dates', 'doc1');
        $doc2 = static::getDatabase()->getDocument('preserve_create_dates', 'doc2');
        $doc3 = static::getDatabase()->getDocument('preserve_create_dates', 'doc3');
        $this->assertEquals($date, $doc1->getAttribute('$createdAt'));
        $this->assertEquals($date, $doc2->getAttribute('$createdAt'));
        $this->assertEquals($date, $doc3->getAttribute('$createdAt'));

        static::getDatabase()->deleteCollection('preserve_create_dates');

        static::getDatabase()->setPreserveDates(false);

        Authorization::reset();
    }

    public function testGetAttributeLimit(): void
    {
        $this->assertIsInt($this->getDatabase()->getLimitForAttributes());
    }
    public function testGetIndexLimit(): void
    {
        $this->assertEquals(58, $this->getDatabase()->getLimitForIndexes());
    }

    public function testGetId(): void
    {
        $this->assertEquals(20, strlen(ID::unique()));
        $this->assertEquals(13, strlen(ID::unique(0)));
        $this->assertEquals(13, strlen(ID::unique(-1)));
        $this->assertEquals(23, strlen(ID::unique(10)));

        // ensure two sequential calls to getId do not give the same result
        $this->assertNotEquals(ID::unique(10), ID::unique(10));
    }

    public function testSharedTablesUpdateTenant(): void
    {
        $database = static::getDatabase();
        $sharedTables = $database->getSharedTables();
        $namespace = $database->getNamespace();
        $schema = $database->getDatabase();

        if (!$database->getAdapter()->getSupportForSchemas()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        if ($database->exists('sharedTables')) {
            $database->setDatabase('sharedTables')->delete();
        }

        $database
            ->setDatabase('sharedTables')
            ->setNamespace('')
            ->setSharedTables(true)
            ->setTenant(null)
            ->create();

        // Create collection
        $database->createCollection(__FUNCTION__, documentSecurity: false);

        $database
            ->setTenant(1)
            ->updateDocument(Database::METADATA, __FUNCTION__, new Document([
                '$id' => __FUNCTION__,
                'name' => 'Scooby Doo',
            ]));

        // Ensure tenant was not swapped
        $doc = $database
            ->setTenant(null)
            ->getDocument(Database::METADATA, __FUNCTION__);

        $this->assertEquals('Scooby Doo', $doc['name']);

        // Reset state
        $database
            ->setSharedTables($sharedTables)
            ->setNamespace($namespace)
            ->setDatabase($schema);
    }
}
