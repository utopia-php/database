<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\PDO;
use Utopia\Database\PermissionType;
use Utopia\Database\Validator\Authorization;

final class PermissionsTest extends TestCase
{
    public function testBatchUpdateParsesEachPermissionTypeOnce(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:', null, null));
        $adapter->setNamespace('permissions');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);
        $adapter->addWriteHook(new Permissions());

        $this->assertTrue($adapter->createCollection('movies'));
        $collection = new Document(['$id' => 'movies']);
        $documents = $adapter->createDocuments($collection, [
            new Document(['$id' => 'first', '$permissions' => [Permission::read(Role::any())]]),
            new Document(['$id' => 'second', '$permissions' => [Permission::read(Role::any())]]),
        ]);
        $updates = new class ([
            '$permissions' => [
                Permission::read(Role::user('reader')),
                Permission::update(Role::user('editor')),
            ],
        ]) extends Document {
            public int $calls = 0;

            #[\Override]
            public function getPermissionsByType(PermissionType $type): array
            {
                $this->calls++;

                return parent::getPermissionsByType($type);
            }
        };

        $adapter->updateDocuments($collection, $updates, $documents);
        $this->assertSame(4, $updates->calls);
    }
}
