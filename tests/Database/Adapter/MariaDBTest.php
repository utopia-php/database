<?php

namespace Utopia\Tests\Adapter;

use PDO;
use Redis;
use Utopia\Database\Database;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Cache\Cache;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Tests\Base;

class MariaDBTest extends Base
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
        return "mariadb";
    }

    /**
     * Return row limit of adapter
     *
     * @return int
     */
    static function getAdapterRowLimit(): int
    {
        return MariaDB::getRowLimit();
    }

    /**
     * @return Adapter
     */
    static function getDatabase(): Database
    {
      if(!is_null(self::$database)) {
        return self::$database;
      }
      $dbHost = 'kdmb-cloud-test-do-user-8650538-0.b.db.ondigitalocean.com';
      $dbPort = '25060';
      $dbUser = 'doadmin';
      $dbPass = 'AzIAei842ArWP4z7';

      $redis = new Redis();
      $redis->connect('redis', 6379);
      $redis->flushAll();

      $database = 
        new Database(
          new MariaDB(
              $dbHost,
              $dbPort,
              $dbUser,
              $dbPass
          ),
          new Cache(new RedisAdapter($redis))
        );

      $database->connect();
      $database->setDefaultDatabase('defaultdb');
      $database->setNamespace('myapp_'.uniqid());

      return self::$database = $database;
    }
}