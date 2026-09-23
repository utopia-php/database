<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

/**
 * The document cache is shared by every caller, so a miss may only become a negative entry once the
 * document is known to be absent. These tests read through an adapter that hides documents the caller
 * cannot read, as the Mongo adapter's getDocument() did, whatever the collection's documentSecurity.
 */
final class MongoFilteredMissCacheTest extends TestCase
{
    private const string COLLECTION = 'profiles';

    public function testMissHiddenByAPermissionFilterIsNotCachedForOtherReaders(): void
    {
        $database = $this->createDatabase($this->createReadFilteringAdapter());

        $this->actAs($database, 'bob');
        $this->assertTrue($database->getDocument(self::COLLECTION, 'alice')->isEmpty());

        $this->actAs($database, 'alice');
        $this->assertSame(
            'Alice',
            $database->getDocument(self::COLLECTION, 'alice')->getAttribute('name'),
            'A reader denied the document must not leave a negative cache entry for a reader who may see it',
        );
    }

    public function testMissOfAnAbsentDocumentIsStillNegativeCached(): void
    {
        $adapter = $this->createReadFilteringAdapter();
        $database = $this->createDatabase($adapter);

        $this->actAs($database, 'bob');
        $this->assertTrue($database->getDocument(self::COLLECTION, 'absent')->isEmpty());
        $reads = $adapter->documentReads;

        $this->assertTrue($database->getDocument(self::COLLECTION, 'absent')->isEmpty());
        $this->assertSame(
            $reads,
            $adapter->documentReads,
            'A document the adapter does not hold must be served from the negative cache',
        );
    }

    private function createReadFilteringAdapter(): CountingMemory
    {
        return new class () extends CountingMemory {
            #[\Override]
            public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                $document = parent::getDocument($collection, $id, $queries, $forUpdate);

                if (
                    $document->isEmpty()
                    || $collection->getId() === Database::METADATA
                    || ! $this->authorization->getStatus()
                    || \array_intersect($document->getRead(), $this->authorization->getRoles()) !== []
                ) {
                    return $document;
                }

                return new Document([]);
            }
        };
    }

    private function createDatabase(CountingMemory $adapter): Database
    {
        $database = new Database($adapter, new Cache(new MemoryCache()));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('filtered_miss_'.\uniqid());
        $database->create();

        $database->getAuthorization()->skip(function () use ($database): void {
            $database->createCollection(new Collection(
                id: self::COLLECTION,
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: [Permission::read(Role::user('alice'))],
                documentSecurity: false,
            ));

            $database->createDocument(self::COLLECTION, new Document([
                '$id' => 'alice',
                '$permissions' => [Permission::read(Role::user('alice'))],
                'name' => 'Alice',
            ]));
        });

        return $database;
    }

    private function actAs(Database $database, string $user): void
    {
        $authorization = $database->getAuthorization();
        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());
        $authorization->addRole(Role::users()->toString());
        $authorization->addRole(Role::user($user)->toString());
    }
}
