<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use ReflectionClass;
use ReflectionMethod;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Validator\Authorization;

class MongoPermissionStringsTest extends TestCase
{
    public function testPeriodInRoleIsLiteralNotRegexWildcard(): void
    {
        $this->assertSame(
            ['read("user:alice.")'],
            $this->permissionStrings(['user:alice.'], Database::PERMISSION_READ)
        );
    }

    public function testMassReadDotPaddingStaysExact(): void
    {
        $role = 'user:a' . \str_repeat('.', 19);

        $this->assertSame(
            ['read("' . $role . '")'],
            $this->permissionStrings([$role], Database::PERMISSION_READ)
        );
    }

    public function testMatchingIsCaseSensitiveAndUsesRequestedType(): void
    {
        $this->assertSame(
            ['update("user:alice")'],
            $this->permissionStrings(['user:alice'], Database::PERMISSION_UPDATE)
        );
    }

    public function testMultipleRolesMapToExactPermissionStrings(): void
    {
        $this->assertSame(
            ['read("user:alice")', 'read("users")'],
            $this->permissionStrings(['user:alice', 'users'], Database::PERMISSION_READ)
        );
    }

    public function testEmptyRolesProduceEmptyList(): void
    {
        $this->assertSame([], $this->permissionStrings([], Database::PERMISSION_READ));
    }

    public function testValuesAreStringsNotRegex(): void
    {
        foreach ($this->permissionStrings(['user:alice.'], Database::PERMISSION_READ) as $value) {
            $this->assertIsString($value);
            $this->assertStringStartsWith('read("', $value);
            $this->assertStringEndsWith('")', $value);
        }
    }

    /**
     * Column-scoped permissions name the column inside the string, so the candidate
     * list has to carry a variant per column. With no columns the output is exactly
     * what it was before column-level permissions existed.
     */
    public function testColumnsAddAVariantPerColumnAlongsideTheUnscopedGrant(): void
    {
        $this->assertSame(
            [
                'read("user:alice")',
                'read("user:alice", "name")',
                'read("user:alice", "salary")',
            ],
            $this->permissionStrings(['user:alice'], Database::PERMISSION_READ, ['name', 'salary'])
        );
    }

    public function testColumnVariantsMatchThePermissionHelperSerialisation(): void
    {
        $strings = $this->permissionStrings(['user:alice'], Database::PERMISSION_READ, ['salary']);

        $this->assertContains(
            \Utopia\Database\Helpers\Permission::read(
                \Utopia\Database\Helpers\Role::user('alice'),
                'salary'
            ),
            $strings
        );
    }

    /**
     * @param list<string> $roles
     * @param list<string> $columns
     * @return list<string>
     */
    private function permissionStrings(array $roles, string $type, array $columns = []): array
    {
        $authorization = new Authorization();
        $authorization->enable();
        $authorization->cleanRoles();
        foreach ($roles as $role) {
            $authorization->addRole($role);
        }

        $adapter = (new ReflectionClass(Mongo::class))->newInstanceWithoutConstructor();
        $adapter->setAuthorization($authorization);

        $method = new ReflectionMethod(Mongo::class, 'permissionStrings');

        $collection = new Document([
            '$id' => 'test',
            'attributes' => \array_map(fn (string $column) => ['key' => $column], $columns),
        ]);

        /** @var list<string> $values */
        $values = $method->invoke($adapter, $type, $collection);

        return $values;
    }
}
