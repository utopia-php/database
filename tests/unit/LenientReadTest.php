<?php

namespace Tests\Unit;

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
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;

final class LenientReadTest extends TestCase
{
    private const string NAMESPACE = 'lenient';

    private const string COLLECTION = 'notes';

    private const string ID = 'note';

    /**
     * @return array<string, array{Closure(): Database}>
     */
    public static function databases(): array
    {
        return [
            'Memory' => [self::memory(...)],
            'SQLite' => [self::sqlite(...)],
        ];
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testGetDocumentDropsAStoredNonStringPermission(Closure $database): void
    {
        $document = $database()->getDocument(self::COLLECTION, self::ID);

        $this->assertSame('stored', $document->getAttribute('title'));
        $this->assertSame([Permission::read(Role::any())], $document->getPermissions());
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testFindDropsAStoredNonStringPermission(Closure $database): void
    {
        $documents = $database()->find(self::COLLECTION);

        $this->assertCount(1, $documents);
        $this->assertSame([Permission::read(Role::any())], $documents[0]->getPermissions());
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testUpdateDocumentOverAStoredNonStringPermission(Closure $database): void
    {
        $database = $database();

        $updated = $database->updateDocument(self::COLLECTION, self::ID, new Document(['title' => 'updated']));

        $this->assertSame('updated', $updated->getAttribute('title'));
        $this->assertSame([Permission::read(Role::any())], $updated->getPermissions());
        $this->assertSame('updated', $database->getDocument(self::COLLECTION, self::ID)->getAttribute('title'));
    }

    /**
     * @param  Closure(): Database  $database
     */
    #[DataProvider('databases')]
    public function testUpdateDocumentsOverAStoredNonStringPermission(Closure $database): void
    {
        $database = $database();

        $this->assertSame(1, $database->updateDocuments(self::COLLECTION, new Document(['title' => 'updated'])));

        $document = $database->getDocument(self::COLLECTION, self::ID);
        $this->assertSame('updated', $document->getAttribute('title'));
        $this->assertSame([Permission::read(Role::any())], $document->getPermissions());
    }

    private static function memory(): Database
    {
        $adapter = new class () extends Memory {
            /**
             * @param  list<mixed>  $permissions
             */
            public function storePermissions(string $collection, string $id, array $permissions): void
            {
                $this->data[$this->key($collection)]['documents'][$this->documentKey($id)][Storage::PERMISSIONS] = $permissions;
            }
        };

        $database = self::seed(self::database($adapter));
        $adapter->storePermissions(self::COLLECTION, self::ID, self::storedPermissions());

        return $database;
    }

    private static function sqlite(): Database
    {
        $pdo = new PDO('sqlite::memory:');
        $database = self::seed(self::database(new SQLite($pdo))->addHook(new Permissions()));

        $table = self::NAMESPACE.'_'.self::COLLECTION;
        $statement = $pdo->prepare('UPDATE `'.$table.'` SET `'.Storage::PERMISSIONS.'` = :permissions WHERE `'.Storage::UID.'` = :id');
        self::assertNotFalse($statement);
        $statement->execute([
            'permissions' => \json_encode(self::storedPermissions(), JSON_THROW_ON_ERROR),
            'id' => self::ID,
        ]);
        self::assertSame(1, $statement->rowCount());

        return $database;
    }

    private static function database(Adapter $adapter): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        return (new Database($adapter, new Cache(new None())))
            ->setAuthorization($authorization)
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE);
    }

    private static function seed(Database $database): Database
    {
        $database->create();
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: true,
        ));
        $database->createDocument(self::COLLECTION, new Document([
            Document::ID => self::ID,
            Document::PERMISSIONS => [Permission::read(Role::any())],
            'title' => 'stored',
        ]));

        return $database;
    }

    /**
     * @return list<mixed>
     */
    private static function storedPermissions(): array
    {
        return [Permission::read(Role::any()), 42, null, Permission::read(Role::any())];
    }
}
