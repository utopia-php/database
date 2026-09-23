<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;

final class MongoLenientReadTest extends TestCase
{
    private const string COLLECTION = 'notes';

    public function testGetDocumentDropsAStoredNonStringPermission(): void
    {
        $adapter = self::adapter([self::row('note')]);

        $document = $adapter->getDocument(new Document([Document::ID => self::COLLECTION]), 'note');

        $this->assertSame('note', $document->getId());
        $this->assertSame([Permission::read(Role::any())], $document->getPermissions());
    }

    public function testFindDropsAStoredNonStringPermissionInEveryBatch(): void
    {
        $adapter = self::adapter([self::row('first')], [self::row('second')]);

        $documents = $adapter->find(new Document([Document::ID => self::COLLECTION]));

        $this->assertSame(['first', 'second'], \array_map(fn (Document $document): string => $document->getId(), $documents));
        foreach ($documents as $document) {
            $this->assertSame([Permission::read(Role::any())], $document->getPermissions());
        }
    }

    private static function row(string $id): stdClass
    {
        return (object) [
            Storage::UID => $id,
            Storage::PERMISSIONS => [Permission::read(Role::any()), 42, null, Permission::read(Role::any())],
            'title' => 'stored',
        ];
    }

    /**
     * @param  list<stdClass>  $firstBatch
     * @param  list<stdClass>  $nextBatch
     */
    private static function adapter(array $firstBatch, array $nextBatch = []): Mongo
    {
        $client = new class ($firstBatch, $nextBatch) extends Client {
            /**
             * @param  list<stdClass>  $firstBatch
             * @param  list<stdClass>  $nextBatch
             */
            public function __construct(private readonly array $firstBatch, private readonly array $nextBatch)
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }

            /**
             * @param  array<mixed>  $filters
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function find(string $collection, array $filters = [], array $options = []): stdClass
            {
                return (object) ['cursor' => (object) ['firstBatch' => $this->firstBatch, 'id' => $this->nextBatch === [] ? 0 : 1]];
            }

            #[\Override]
            public function getMore(int $cursorId, string $collection, int $batchSize = 25): stdClass
            {
                return (object) ['cursor' => (object) ['nextBatch' => $this->nextBatch, 'id' => 0]];
            }
        };

        $adapter = new Mongo($client);
        $adapter->setAuthorization(new Authorization());
        $adapter->setNamespace('lenient');

        return $adapter;
    }
}
