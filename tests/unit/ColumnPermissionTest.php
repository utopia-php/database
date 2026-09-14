<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
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
     * A column-scoped permission must still resolve to a bare role, or every
     * existing document-level authorization check silently breaks.
     */
    public function testDocumentPermissionsByTypeReturnsRolesOnly(): void
    {
        $document = new Document(['$permissions' => [
            'read("any")',
            'read("user:1", "salary")',
            'update("user:1", "name")',
            'delete("user:1")',
        ]]);

        $this->assertSame(['any', 'user:1'], \array_values($document->getRead()));
        $this->assertSame(['user:1'], \array_values($document->getUpdate()));
        $this->assertSame(['user:1'], \array_values($document->getDelete()));
    }

    public function testDocumentPermissionsByTypeWithColumns(): void
    {
        $document = new Document(['$permissions' => [
            'read("any")',
            'read("user:1", "salary")',
        ]]);

        $this->assertSame([
            ['role' => 'any', 'column' => Permission::COLUMN_ALL],
            ['role' => 'user:1', 'column' => 'salary'],
        ], $document->getPermissionsByTypeWithColumns('read'));
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
