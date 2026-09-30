<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Validator\Authorization;

final class SQLiteDigitLeadingNamespaceTest extends TestCase
{
    private const string COLLECTION = 'posts';

    #[DataProvider('namespaces')]
    public function testDocumentSecurityReadsWorkUnderTheNamespace(string $namespace): void
    {
        $database = $this->database($namespace);

        $this->assertSame(['public'], \array_map(
            static fn (Document $document): string => $document->getId(),
            $database->find(self::COLLECTION),
        ));
        $this->assertSame(1, $database->count(self::COLLECTION));
        $this->assertSame('public', $database->getDocument(self::COLLECTION, 'public')->getId());
        $this->assertTrue($database->getDocument(self::COLLECTION, 'private')->isEmpty());
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function namespaces(): iterable
    {
        yield 'a leading letter' => ['ns1'];
        yield 'a leading digit' => ['1ns'];
        yield 'a leading hyphen' => ['-ns'];
    }

    private function database(string $namespace): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = (new Database(new SQLite(new PDO('sqlite::memory:', null, null, SQLite::getPDOAttributes())), new Cache(new None())))
            ->setAuthorization($authorization)
            ->setDatabase('digit_leading')
            ->setNamespace($namespace)
            ->addHook(new Permissions());
        $database->create();

        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));
        $database->createDocument(self::COLLECTION, new Document([
            '$id' => 'public',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Readable',
        ]));
        $database->createDocument(self::COLLECTION, new Document([
            '$id' => 'private',
            '$permissions' => [Permission::read(Role::user('owner'))],
            'title' => 'Hidden',
        ]));

        return $database;
    }
}
