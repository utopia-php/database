<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Hook\Mongo\PermissionFilter;
use Utopia\Database\PermissionType;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;

class MongoPermissionStringsTest extends TestCase
{
    public function testPeriodInRoleIsLiteralNotRegexWildcard(): void
    {
        $this->assertSame(
            ['read("user:alice.")'],
            $this->permissionStrings(['user:alice.'], PermissionType::Read)
        );
    }

    public function testMassReadDotPaddingStaysExact(): void
    {
        $role = 'user:a'.\str_repeat('.', 19);

        $this->assertSame(
            ['read("'.$role.'")'],
            $this->permissionStrings([$role], PermissionType::Read)
        );
    }

    public function testMatchingIsCaseSensitiveAndUsesRequestedType(): void
    {
        $this->assertSame(
            ['update("user:alice")'],
            $this->permissionStrings(['user:alice'], PermissionType::Update)
        );
    }

    public function testMultipleRolesMapToExactPermissionStrings(): void
    {
        $this->assertSame(
            ['read("user:alice")', 'read("users")'],
            $this->permissionStrings(['user:alice', 'users'], PermissionType::Read)
        );
    }

    public function testEmptyRolesProduceEmptyList(): void
    {
        $this->assertSame([], $this->permissionStrings([], PermissionType::Read));
    }

    public function testValuesAreStringsNotRegex(): void
    {
        foreach ($this->permissionStrings(['user:alice.'], PermissionType::Read) as $value) {
            $this->assertStringStartsWith('read("', $value);
            $this->assertStringEndsWith('")', $value);
        }
    }

    /**
     * @param list<string> $roles
     * @return list<string>
     */
    private function permissionStrings(array $roles, PermissionType $type): array
    {
        $authorization = new Authorization();
        $authorization->enable();
        $authorization->cleanRoles();
        foreach ($roles as $role) {
            $authorization->addRole($role);
        }

        $filters = (new PermissionFilter($authorization))->applyFilters([], 'documents', $type->value);
        $permissionFilter = $filters[Storage::PERMISSIONS] ?? null;
        if (! \is_array($permissionFilter)) {
            return [];
        }

        $values = $permissionFilter['$in'] ?? [];
        if (! \is_array($values)) {
            return [];
        }

        $strings = [];
        foreach ($values as $value) {
            if (! \is_string($value)) {
                $this->fail('Permission $in values must be strings');
            }
            $strings[] = $value;
        }

        return $strings;
    }
}
