<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

final class MemoryAdapterTest extends TestCase
{
    private const string COLLECTION = 'notes';

    private const string DOCUMENT = 'note';

    private const string ALICE = 'alice';

    private const string BOB = 'bob';

    private Authorization $authorization;

    protected function setUp(): void
    {
        $this->authorization = new Authorization();
        $this->authorization->addRole(Role::any()->toString());
    }

    public function testDeleteWithAnotherCasingRemovesTheGrants(): void
    {
        $database = $this->memory();
        $this->createNotes($database);
        $database->createDocument(self::COLLECTION, $this->note([self::ALICE]));

        $this->assertTrue($database->deleteDocument(self::COLLECTION, \strtoupper(self::DOCUMENT)));
        $database->createDocument(self::COLLECTION, $this->note([self::BOB]));

        $this->assertSame([], $this->readableBy($database, self::ALICE), 'A delete under another casing must remove the deleted document\'s grants');
        $this->assertSame([self::DOCUMENT], $this->readableBy($database, self::BOB));
    }

    private function database(Adapter $adapter): Database
    {
        return (new Database($adapter, new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('memory_adapter')
            ->setNamespace('memory_adapter_'.\uniqid());
    }

    private function memory(): Database
    {
        $database = $this->database(new Memory());
        $database->create();

        return $database;
    }

    private function createNotes(Database $database): void
    {
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            documentSecurity: true,
        ));
    }

    /**
     * @param  list<string>  $readers
     */
    private function note(array $readers): Document
    {
        return $this->readers($readers)
            ->setAttribute('$id', self::DOCUMENT)
            ->setAttribute('title', 'first');
    }

    /**
     * @param  list<string>  $readers
     */
    private function readers(array $readers): Document
    {
        return new Document([
            '$permissions' => \array_map(
                static fn (string $reader): string => Permission::read(Role::user($reader)),
                $readers,
            ),
        ]);
    }

    /**
     * @return list<string>
     */
    private function readableBy(Database $database, string $reader): array
    {
        $roles = $this->authorization->getRoles();
        $this->authorization->cleanRoles();
        $this->authorization->addRole(Role::user($reader)->toString());

        try {
            return \array_values(\array_map(
                static fn (Document $document): string => $document->getId(),
                $database->find(self::COLLECTION),
            ));
        } finally {
            $this->authorization->cleanRoles();
            foreach ($roles as $role) {
                $this->authorization->addRole($role);
            }
        }
    }
}
