<?php

namespace Utopia\Tests\Adapter;

use PDO;
use Redis;
use Utopia\Cache\Cache;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Adapter\MySQL;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Tests\Base;

use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

class MySQLTest extends Base
{
    /**
     * @var Database
     */
    static $database = null;

    // TODO@kodumbeats hacky way to identify adapters for tests
    // Remove once all methods are implemented
    /**
     * Return name of adapter
     *
     * @return string
     */
    static function getAdapterName(): string
    {
        return "mysql";
    }

    /**
     * Return row limit of adapter
     *
     * @return int
     */
    static function getAdapterRowLimit(): int
    {
        return MySQL::getRowLimit();
    }

    /**
     * 
     * @return int 
     */
    static function getUsedIndexes(): int
    {
        return MySQL::getNumberOfDefaultIndexes();
    }

    /**
     * @reture Adapter
     */
    static function getDatabase(): Database
    {
        if(!is_null(self::$database)) {
            return self::$database;
        }

        $dbHost = 'mysql';
        $dbPort = '3307';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, array(
            PDO::ATTR_TIMEOUT => 3, // Seconds
            PDO::ATTR_PERSISTENT => true,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_EMULATE_PREPARES => true,
            PDO::ATTR_STRINGIFY_FETCHES => true,
        ));

        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new MySQL($pdo), $cache);
        $database->setDefaultDatabase('utopiaTests');
        $database->setNamespace('myapp_'.uniqid());

        return self::$database = $database;
    }

    /**
     * @depends testUpdateDocument
     */
    public function testFind(Document $document)
    {
        static::getDatabase()->createCollection('movies');

        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'name', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'director', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'year', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'price', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'active', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'generes', Database::VAR_STRING, 32, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'director-email', Database::VAR_STRING, 32, false));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'total-revenue', Database::VAR_INTEGER, 0, false));

        static::getDatabase()->createDocument('movies', new Document([
            '$id' => 'frozen',
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Frozen',
            'director' => 'Chris Buck & Jennifer Lee',
            'director-email' => 'chris@gmail.com',
            'year' => 2013,
            'price' => 39.50,
            'active' => true,
            'generes' => ['animation', 'kids'],
            'total-revenue' => 19282020
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Frozen II',
            'director' => 'Chris Buck & Jennifer Lee',
            'director-email' => 'frozenII@gmail.com',
            'year' => 2019,
            'price' => 39.50,
            'active' => true,
            'generes' => ['animation', 'kids'],
            'total-revenue' => 20438921
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Captain America: The First Avenger',
            'director' => 'Joe Johnston',
            'director-email' => 'joe@gmail.com',
            'year' => 2011,
            'price' => 25.94,
            'active' => true,
            'generes' => ['science fiction', 'action', 'comics'],
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Captain Marvel',
            'director' => 'Anna Boden & Ryan Fleck',
            'director-email' => 'anna@gmail.com',
            'year' => 2019,
            'price' => 25.99,
            'active' => true,
            'generes' => ['science fiction', 'action', 'comics'],
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$read' => ['role:all', 'user1', 'user2'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Work in Progress',
            'director' => 'TBD',
            'director-email' => 'tbd@gmail.com',
            'year' => 2025,
            'price' => 0.0,
            'active' => false,
            'generes' => [],
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$read' => ['userx'],
            '$write' => ['role:all', 'user1x', 'user2x'],
            'name' => 'Work in Progress 2',
            'director' => 'TBD',
            'director-email' => 'tbd2@gmail.com',
            'year' => 2026,
            'price' => 0.0,
            'active' => false,
            'generes' => [],
        ]));

        /**
         * Check hyphen column
         */
        $documents = static::getDatabase()->find('movies', 
        [
            new Query('director-email', Query::TYPE_EQUAL, ['tbd@gmail.com']),
        ]);
        $this->assertEquals(1, count($documents));
    }


    /**
     * @depends testFind
     */
    public function testCount()
    {
        $count = static::getDatabase()->count('movies');
        $this->assertEquals(5, $count);
        
        $count = static::getDatabase()->count('movies', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(2, $count);
        
        Authorization::unsetRole('userx');
        $count = static::getDatabase()->count('movies');
        $this->assertEquals(5, $count);
        
        Authorization::disable();
        $count = static::getDatabase()->count('movies');
        $this->assertEquals(6, $count);
        Authorization::reset();
        
        Authorization::disable();
        $count = static::getDatabase()->count('movies', [], 3);
        $this->assertEquals(3, $count);
        Authorization::reset();

        /**
         * Count using hyphen columns
         */
        $count = static::getDatabase()->count('movies', [new Query('director-email', Query::TYPE_EQUAL, ['frozenII@gmail.com']),]);
        $this->assertEquals(1, $count);

        /**
         * Test that OR queries are handled correctly
         */
        Authorization::disable();
        $count = static::getDatabase()->count('movies', [
            new Query('director', Query::TYPE_EQUAL, ['TBD', 'Joe Johnston']),
            new Query('year', Query::TYPE_EQUAL, [2025]),
        ]);
        $this->assertEquals(1, $count);
        Authorization::reset();
    }

    /**
     * @depends testFind
     */
    public function testSum()
    {
        Authorization::setRole('userx');
        $sum = static::getDatabase()->sum('movies', 'year', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(2019+2019, $sum);
        $sum = static::getDatabase()->sum('movies', 'year');
        $this->assertEquals(2013+2019+2011+2019+2025+2026, $sum);
        $sum = static::getDatabase()->sum('movies', 'price', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(round(39.50+25.99, 2), round($sum, 2));
        $sum = static::getDatabase()->sum('movies', 'price', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(round(39.50+25.99, 2), round($sum, 2));
        
        $sum = static::getDatabase()->sum('movies', 'year', [new Query('year', Query::TYPE_EQUAL, [2019])], 1);
        $this->assertEquals(2019, $sum);

        $sum = static::getDatabase()->sum('movies', 'total-revenue', [new Query('director', Query::TYPE_EQUAL, ['Chris Buck & Jennifer Lee'])],);
        $this->assertEquals(19282020+20438921, $sum);

        Authorization::unsetRole('userx');
        $sum = static::getDatabase()->sum('movies', 'year', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(2019+2019, $sum);
        $sum = static::getDatabase()->sum('movies', 'year');
        $this->assertEquals(2013+2019+2011+2019+2025, $sum);
        $sum = static::getDatabase()->sum('movies', 'price', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(round(39.50+25.99, 2), round($sum, 2));
        $sum = static::getDatabase()->sum('movies', 'price', [new Query('year', Query::TYPE_EQUAL, [2019]),]);
        $this->assertEquals(round(39.50+25.99, 2), round($sum, 2));
    }

    /**
     * Return keywords reserved by database backend
     * Refference: https://mariadb.com/kb/en/reserved-words/
     *
     * @return string[]
     */
    static function getReservedKeywords(): array
    {
        // Same as MariaDB
        return MariaDBTest::getReservedKeywords();
    }
}