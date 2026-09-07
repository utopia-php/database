<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

final class SQLiteForUpdateDocumentTest extends TestCase
{
    public function testVersionedUpdateOnRawSqlitePdoDoesNotTreatColumnIndexesAsAttributes(): void
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $database
            ->setDatabase('for_update')
            ->setNamespace('for_update_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->create();

        $database->createCollection(new Collection(
            id: 'migrations',
            attributes: [
                Attribute::string('status'),
                Attribute::string('stage'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::delete(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ));

        $active = $database->createDocument('migrations', new Document([
            '$id' => 'migration',
            'status' => 'processing',
            'stage' => 'processing',
        ]));

        $locked = $database->getDocument('migrations', $active->getId(), forUpdate: true);
        $this->assertArrayNotHasKey(0, $locked->getArrayCopy());
        $this->assertNull($locked->getAttribute('0'));

        $database->setPreserveDates(true);
        $newer = $database->updateDocument('migrations', $active->getId(), new Document([
            '$updatedAt' => $active->getUpdatedAt(),
            'stage' => 'migrating',
        ]), expectedVersion: $active->getVersion());

        $this->assertSame('migrating', $newer->getAttribute('stage'));
        $this->assertSame('processing', $newer->getAttribute('status'));
        $this->assertSame($active->getUpdatedAt(), $newer->getUpdatedAt());
        $this->assertNotSame($active->getVersion(), $newer->getVersion());
    }
}
