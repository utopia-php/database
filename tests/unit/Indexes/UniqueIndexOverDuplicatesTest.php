<?php

namespace Tests\Unit\Indexes;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class UniqueIndexOverDuplicatesTest extends TestCase
{
    private const string COLLECTION = 'pets';

    /**
     * @return array<string, array{Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'Memory' => [static fn (): Adapter => new Memory()],
            'SQLite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
        ];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testUniqueIndexOverDuplicateValuesIsRefusedWithoutMetadata(Closure $adapter): void
    {
        $database = $this->database($adapter());

        foreach (['name', 'age'] as $attribute) {
            $error = null;
            try {
                $database->createIndex(self::COLLECTION, Index::unique(key: 'unique_'.$attribute, attributes: [$attribute]));
            } catch (Throwable $caught) {
                $error = $caught;
            }

            $this->assertInstanceOf(UniqueException::class, $error, 'A unique index on '.$attribute.' over duplicate values must be refused as Unique');
            $this->assertSame([], $database->getCollection(self::COLLECTION)->indexes(), 'A refused unique index on '.$attribute.' must leave no metadata behind');
        }

        $database->createDocument(self::COLLECTION, new Document(['name' => 'chester', 'age' => 7]));
        $this->assertSame(3, $database->count(self::COLLECTION), 'No unique index may be left in the schema to refuse a third duplicate');
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('unique_over_duplicates')
            ->setNamespace('unique_over_duplicates_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();

        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'age'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
            documentSecurity: false,
        ));

        foreach (['first', 'second'] as $id) {
            $database->createDocument(self::COLLECTION, new Document(['$id' => $id, 'name' => 'chester', 'age' => 7]));
        }

        return $database;
    }
}
