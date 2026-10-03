<?php

namespace Tests\Unit\Documents;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;

final class CaseOnlyRenameTest extends TestCase
{
    private const string COLLECTION = 'renames';

    /**
     * @return array<string, array{Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
            'memory' => [static fn (): Adapter => new Memory()],
        ];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCaseOnlyRenameIsPerformed(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createDocument(self::COLLECTION, new Document([
            '$id' => 'abc',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'renamed',
        ]));

        $renamed = $database->updateDocument(self::COLLECTION, 'abc', new Document(['$id' => 'ABC']));

        $this->assertSame('ABC', $renamed->getId());
        $this->assertSame(['ABC'], \array_map(static fn (Document $document): string => $document->getId(), $database->find(self::COLLECTION)));
        $stored = $database->getDocument(self::COLLECTION, 'ABC');
        $this->assertSame('ABC', $stored->getId());
        $this->assertSame('renamed', $stored->getAttribute('name'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCaseOnlyRenameKeepsDocumentLevelPermissionsWithTheDocument(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createDocument(self::COLLECTION, new Document([
            '$id' => 'abc',
            '$permissions' => [
                Permission::read(Role::user('alice')),
                Permission::update(Role::user('alice')),
            ],
            'name' => 'private',
        ]));

        $database->updateDocument(self::COLLECTION, 'abc', new Document(['$id' => 'ABC']));

        $authorization = $database->getAuthorization();
        $authorization->cleanRoles();
        $authorization->addRole(Role::user('alice')->toString());

        $this->assertSame('ABC', $database->getDocument(self::COLLECTION, 'ABC')->getId());
        $this->assertSame(['ABC'], \array_map(static fn (Document $document): string => $document->getId(), $database->find(self::COLLECTION)));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRenameOntoAnotherDocumentIdInDifferentCaseStillConflicts(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'abc']));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'xyz']));

        $this->expectException(DuplicateException::class);

        $database->updateDocument(self::COLLECTION, 'abc', new Document(['$id' => 'XYZ']));
    }

    public function testCaseOnlyRenameMovesSqlPermissionRowsToTheNewId(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $database = $this->database(new SQLite($pdo));
        $database->createDocument(self::COLLECTION, new Document([
            '$id' => 'abc',
            '$permissions' => [Permission::read(Role::user('alice'))],
        ]));

        $database->updateDocument(self::COLLECTION, 'abc', new Document(['$id' => 'ABC']));

        $rows = $pdo->query('SELECT _document, _type, _permission FROM "'.$database->getNamespace().'_'.self::COLLECTION.'_perms"');
        $this->assertNotFalse($rows);
        $this->assertSame(
            [['_document' => 'ABC', '_type' => 'read', '_permission' => 'user:alice']],
            $rows->fetchAll(PDO::FETCH_ASSOC),
        );
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->addHook(new Permissions());
        $database
            ->setDatabase('case_rename')
            ->setNamespace('case_rename_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string('name', size: 32, required: false)],
            permissions: [
                Permission::create(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: true,
        ));

        return $database;
    }
}
