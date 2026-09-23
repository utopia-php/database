<?php

namespace Tests\Unit\Event;

use PDO;
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
use Utopia\Database\Validator\Authorization;

final class HookFixture
{
    public const string COLLECTION = 'posts';

    public static function sqlite(): Database
    {
        return self::database(new SQLite(new PDO('sqlite::memory:')));
    }

    public static function memory(): Database
    {
        return self::database(new Memory());
    }

    /**
     * @param  list<string>  $ids
     */
    public static function seed(Database $database, array $ids): void
    {
        foreach ($ids as $index => $id) {
            $database->createDocument(self::COLLECTION, new Document([
                Document::ID => $id,
                'title' => $id,
                'views' => $index + 1,
            ]));
        }
    }

    public static function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('hooks')
            ->setNamespace('hooks_'.\uniqid());
        $database->create();

        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'title', size: 64),
                Attribute::integer(key: 'views'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ));

        return $database;
    }
}
