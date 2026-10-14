<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Collection\Update;
use Utopia\Database\Permission;
use Utopia\Database\Role;

final class CollectionUpdateTest extends TestCase
{
    public function testDefaultsChangeNothing(): void
    {
        $update = new Update();

        $this->assertNull($update->permissions);
        $this->assertNull($update->documentSecurity);
    }

    public function testCarriesOnlyTheGivenChanges(): void
    {
        $permissions = new Update(permissions: [Permission::read(Role::any())]);
        $security = new Update(documentSecurity: false);

        $this->assertSame(['read("any")'], $permissions->permissions);
        $this->assertNull($permissions->documentSecurity);
        $this->assertNull($security->permissions);
        $this->assertFalse($security->documentSecurity);
    }

    public function testAnEmptyPermissionListIsAChange(): void
    {
        $update = new Update(permissions: []);

        $this->assertSame([], $update->permissions);
    }

    public function testPropertiesAreReadonly(): void
    {
        $update = new Update(documentSecurity: true);

        $this->expectException(\Error::class);

        /** @phpstan-ignore assign.propertyProtectedSet */
        $update->documentSecurity = false;
    }
}
