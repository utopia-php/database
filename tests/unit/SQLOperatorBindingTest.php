<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Operator;
use Utopia\Database\PDO;
use Utopia\Database\Permission;
use Utopia\Database\Role;

final class SQLOperatorBindingTest extends TestCase
{
    public function testUpsertBindsOperatorsToDatabaseStatement(): void
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:', null, null)), new Cache(new None()));
        $database->setDatabase('operators')->setNamespace('operators');
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(Collection::create(
            id: 'scores',
            attributes: [Attribute::integer(key: 'value')],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: false,
        ));
        $database->createDocument('scores', new Document(['$id' => 'first', 'value' => 1]));

        $database->upsertDocuments('scores', [new Document(['$id' => 'first', 'value' => Operator::increment(2)])]);

        $this->assertSame(3, $database->getDocument('scores', 'first')->getAttribute('value'));
    }
}
