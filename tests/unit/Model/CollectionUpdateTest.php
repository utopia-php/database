<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\TestCase;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

final class CollectionUpdateTest extends TestCase
{
    public function testDefaultsChangeNothing(): void
    {
        $update = new CollectionUpdate();

        $this->assertNull($update->permissions);
        $this->assertNull($update->documentSecurity);
    }

    public function testCarriesOnlyTheGivenChanges(): void
    {
        $permissions = new CollectionUpdate(permissions: [Permission::read(Role::any())]);
        $security = new CollectionUpdate(documentSecurity: false);

        $this->assertSame(['read("any")'], $permissions->permissions);
        $this->assertNull($permissions->documentSecurity);
        $this->assertNull($security->permissions);
        $this->assertFalse($security->documentSecurity);
    }

    public function testAnEmptyPermissionListIsAChange(): void
    {
        $update = new CollectionUpdate(permissions: []);

        $this->assertSame([], $update->permissions);
    }

    public function testPropertiesAreReadonly(): void
    {
        $update = new CollectionUpdate(documentSecurity: true);

        $this->expectException(\Error::class);

        /** @phpstan-ignore assign.propertyProtectedSet */
        $update->documentSecurity = false;
    }
}
