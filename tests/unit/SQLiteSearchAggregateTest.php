<?php

namespace Tests\Unit;

use PDO;
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
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\IndexType;

final class SQLiteSearchAggregateTest extends TestCase
{
    private const string COLLECTION = 'notes';

    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $this->database
            ->setAuthorization(new Authorization())
            ->setDatabase('search')
            ->setNamespace('search_'.\uniqid());
        $this->database->create();
        $this->database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'body', size: 128),
                Attribute::integer(key: 'views'),
            ],
            indexes: [new Index(key: 'body_search', type: IndexType::Fulltext, attributes: ['body'])],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
            documentSecurity: false,
        ));
        $this->database->createDocument(self::COLLECTION, new Document([Document::ID => 'match', 'body' => 'apple pie', 'views' => 3]));
        $this->database->createDocument(self::COLLECTION, new Document([Document::ID => 'other', 'body' => 'banana bread', 'views' => 5]));
    }

    public function testCountAppliesASearch(): void
    {
        $this->assertSame(1, $this->database->count(self::COLLECTION, [Query::search('body', 'apple')]));
        $this->assertSame(1, $this->database->count(self::COLLECTION, [Query::notSearch('body', 'apple')]));
    }

    public function testSumAppliesASearch(): void
    {
        $this->assertSame(3, $this->database->sum(self::COLLECTION, 'views', [Query::search('body', 'apple')]));
        $this->assertSame(5, $this->database->sum(self::COLLECTION, 'views', [Query::notSearch('body', 'apple')]));
    }
}
