<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Permissions;

class ColumnPermissionTest extends TestCase
{
    public function testParseWithoutColumnGrantsEveryColumn(): void
    {
        foreach (['read("any")', 'read("users")', 'read("user:123")', 'read("team:123/admin")'] as $string) {
            $permission = Permission::parse($string);

            $this->assertSame(Permission::COLUMN_ALL, $permission->getColumn());
            $this->assertTrue($permission->isForAllColumns());
            $this->assertSame($string, $permission->toString());
        }
    }

    public function testParseWithColumn(): void
    {
        $permission = Permission::parse('read("user:123", "salary")');

        $this->assertSame('read', $permission->getPermission());
        $this->assertSame('user', $permission->getRole());
        $this->assertSame('123', $permission->getIdentifier());
        $this->assertSame('', $permission->getDimension());
        $this->assertSame('salary', $permission->getColumn());
        $this->assertFalse($permission->isForAllColumns());
    }

    public function testParseWithColumnAndDimension(): void
    {
        $permission = Permission::parse('update("team:abc/owner", "salary")');

        $this->assertSame('update', $permission->getPermission());
        $this->assertSame('team', $permission->getRole());
        $this->assertSame('abc', $permission->getIdentifier());
        $this->assertSame('owner', $permission->getDimension());
        $this->assertSame('salary', $permission->getColumn());
    }

    /**
     * @return array<string, array{string}>
     */
    public static function roundTripProvider(): array
    {
        return [
            'no column' => ['read("any")'],
            'column' => ['read("user:123", "salary")'],
            'dimension and column' => ['read("team:abc/owner", "salary")'],
            'update column' => ['update("user:123", "name")'],
            'create column' => ['create("users", "name")'],
        ];
    }

    /**
     * @dataProvider roundTripProvider
     */
    public function testRoundTrip(string $string): void
    {
        $this->assertSame($string, Permission::parse($string)->toString());
    }

    public function testFactories(): void
    {
        $this->assertSame('read("user:123")', Permission::read(Role::user('123')));
        $this->assertSame('read("user:123", "salary")', Permission::read(Role::user('123'), 'salary'));
        $this->assertSame('update("team:abc/owner", "name")', Permission::update(Role::team('abc', 'owner'), 'name'));
        $this->assertSame('create("users", "name")', Permission::create(Role::users(), 'name'));
    }

    public function testAggregatePreservesColumn(): void
    {
        $aggregated = Permission::aggregate(['read("user:1", "name")']);

        $this->assertSame(['read("user:1", "name")'], $aggregated);
    }

    public function testEmptyColumnIsRejected(): void
    {
        $this->expectException(DatabaseException::class);
        Permission::parse('read("user:1", "")');
    }

    public function testWildcardColumnIsRejected(): void
    {
        $this->expectException(DatabaseException::class);
        Permission::parse('read("user:1", "*")');
    }

    /**
     * A column-scoped permission still grants its role ordinary row-level access --
     * the column narrows what is returned, it does not withhold the row. Asserted
     * through a read rather than through the shape of the extracted permission list.
     */
    public function testColumnScopedGrantStillGrantsTheRowToThatRole(): void
    {
        $authorization = new Authorization();

        $database = new Database(new Memory(), new Cache(new NoCache()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('columnPermissions')
            ->setNamespace('cpt_' . \uniqid());

        $database->create();

        $authorization->skip(function () use ($database) {
            $database->createCollection('employees', documentSecurity: true, columnSecurity: true, permissions: []);
            $database->createAttribute('employees', 'name', Database::VAR_STRING, 128, false);
            $database->createAttribute('employees', 'salary', Database::VAR_INTEGER, 8, false);

            $database->createDocument('employees', new Document([
                '$id' => 'e1',
                '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
                'name' => 'Bob',
                'salary' => 100000,
            ]));
        });

        $authorization->cleanRoles();
        $authorization->addRole('user:hr');

        $document = $database->getDocument('employees', 'e1');

        $this->assertFalse($document->isEmpty(), 'a column-scoped grant must still make the row visible');
        $this->assertSame(100000, $document->getAttribute('salary'));
        $this->assertNull($document->getAttribute('name'));

        $authorization->cleanRoles();
        $authorization->addRole('user:other');

        $this->assertTrue($database->getDocument('employees', 'e1')->isEmpty());
    }

    public function testValidatorAcceptsColumnScopedReadCreateUpdate(): void
    {
        $validator = new Permissions();

        $this->assertTrue($validator->isValid([
            'read("user:1", "salary")',
            'create("users", "name")',
            'update("team:abc/owner", "name")',
        ]), $validator->getDescription());
    }

    public function testValidatorRejectsColumnScopedDelete(): void
    {
        $validator = new Permissions();

        $this->assertFalse($validator->isValid(['delete("user:1", "salary")']));
        $this->assertStringContainsString('cannot be scoped to a column', $validator->getDescription());
    }

    public function testValidatorRejectsColumnScopedWrite(): void
    {
        $validator = new Permissions();

        $this->assertFalse($validator->isValid(['write("user:1", "salary")']));
        $this->assertStringContainsString('cannot be scoped to a column', $validator->getDescription());
    }

    public function testValidatorRejectsUnknownColumnWhenColumnsGiven(): void
    {
        $validator = new Permissions(columns: ['name', 'email']);

        $this->assertTrue($validator->isValid(['read("user:1", "name")']), $validator->getDescription());
        $this->assertFalse($validator->isValid(['read("user:1", "salary")']));
        $this->assertStringContainsString('does not exist', $validator->getDescription());
    }

    public function testValidatorRejectsInvalidColumnKey(): void
    {
        $validator = new Permissions();

        $this->assertFalse($validator->isValid(['read("user:1", "_internal")']));
        $this->assertStringContainsString('not a valid column key', $validator->getDescription());
    }
}
