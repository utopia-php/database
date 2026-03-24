<?php

namespace Tests\Unit\Authorization;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Authorization\Input;

class AuthorizationTest extends TestCase
{
    private Authorization $auth;

    protected function setUp(): void
    {
        $this->auth = new Authorization();
    }

    public function testDefaultRolesContainAny(): void
    {
        $roles = $this->auth->getRoles();
        $this->assertContains('any', $roles);
        $this->assertCount(1, $roles);
    }

    public function testIsValidWithMatchingRole(): void
    {
        $this->auth->addRole('user:123');
        $input = new Input('read', ['user:123']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testIsValidWithNonMatchingRole(): void
    {
        $this->auth->addRole('user:123');
        $input = new Input('read', ['user:456']);
        $this->assertFalse($this->auth->isValid($input));
    }

    public function testIsValidWithAnyRoleMatchesAllPermissions(): void
    {
        $input = new Input('read', ['any']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testIsValidReturnsFalseWithEmptyPermissions(): void
    {
        $input = new Input('read', []);
        $this->assertFalse($this->auth->isValid($input));
        $this->assertStringContainsString('No permissions provided', $this->auth->getDescription());
    }

    public function testIsValidReturnsFalseWithInvalidInput(): void
    {
        $this->assertFalse($this->auth->isValid('not-an-input'));
        $this->assertEquals('Invalid input provided', $this->auth->getDescription());
    }

    public function testAddRole(): void
    {
        $this->auth->addRole('user:123');
        $this->assertTrue($this->auth->hasRole('user:123'));
        $this->assertContains('user:123', $this->auth->getRoles());
    }

    public function testRemoveRole(): void
    {
        $this->auth->addRole('user:123');
        $this->assertTrue($this->auth->hasRole('user:123'));

        $this->auth->removeRole('user:123');
        $this->assertFalse($this->auth->hasRole('user:123'));
    }

    public function testGetRolesReturnsAllRoles(): void
    {
        $this->auth->addRole('user:123');
        $this->auth->addRole('team:456');
        $this->auth->addRole('users');

        $roles = $this->auth->getRoles();
        $this->assertContains('any', $roles);
        $this->assertContains('user:123', $roles);
        $this->assertContains('team:456', $roles);
        $this->assertContains('users', $roles);
        $this->assertCount(4, $roles);
    }

    public function testSkipBypassesAuthorization(): void
    {
        $this->auth->cleanRoles();

        $input = new Input('read', ['user:999']);
        $this->assertFalse($this->auth->isValid($input));

        $result = $this->auth->skip(function () use ($input) {
            return $this->auth->isValid($input);
        });

        $this->assertTrue($result);
    }

    public function testSkipRestoresStatusAfterCallback(): void
    {
        $this->assertTrue($this->auth->getStatus());

        $this->auth->skip(function () {
            $this->assertFalse($this->auth->getStatus());
        });

        $this->assertTrue($this->auth->getStatus());
    }

    public function testSkipRestoresStatusOnException(): void
    {
        $this->assertTrue($this->auth->getStatus());

        try {
            $this->auth->skip(function () {
                throw new \RuntimeException('test');
            });
        } catch (\RuntimeException) {
        }

        $this->assertTrue($this->auth->getStatus());
    }

    public function testIsValidWithMultipleRoles(): void
    {
        $this->auth->addRole('user:123');
        $this->auth->addRole('team:456');

        $input = new Input('read', ['team:456']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testIsValidWithMultiplePermissionsMatchesFirst(): void
    {
        $this->auth->addRole('user:123');

        $input = new Input('read', ['user:123', 'team:456']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testIsValidWithMultiplePermissionsMatchesLast(): void
    {
        $this->auth->addRole('team:456');

        $input = new Input('read', ['user:123', 'team:456']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testIsValidWithGuestsRole(): void
    {
        $this->auth->addRole('guests');

        $input = new Input('read', ['guests']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testIsValidWithUsersRole(): void
    {
        $this->auth->addRole('users');

        $input = new Input('read', ['users']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testIsValidWithDimensionalRole(): void
    {
        $this->auth->addRole('user:123/admin');

        $input = new Input('read', ['user:123/admin']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testDimensionalRoleDoesNotMatchWithoutDimension(): void
    {
        $this->auth->addRole('user:123/admin');

        $input = new Input('read', ['user:123']);
        $this->assertFalse($this->auth->isValid($input));
    }

    public function testNonDimensionalRoleDoesNotMatchWithDimension(): void
    {
        $this->auth->addRole('user:123');

        $input = new Input('read', ['user:123/admin']);
        $this->assertFalse($this->auth->isValid($input));
    }

    public function testGetDescriptionOnFailure(): void
    {
        $this->auth->cleanRoles();
        $this->auth->addRole('user:123');

        $input = new Input('read', ['team:456']);
        $this->assertFalse($this->auth->isValid($input));

        $description = $this->auth->getDescription();
        $this->assertStringContainsString('Missing "read" permission', $description);
        $this->assertStringContainsString('team:456', $description);
    }

    public function testGetDescriptionOnEmptyPermissions(): void
    {
        $input = new Input('write', []);
        $this->assertFalse($this->auth->isValid($input));
        $this->assertStringContainsString("No permissions provided for action 'write'", $this->auth->getDescription());
    }

    public function testCleanRolesRemovesAll(): void
    {
        $this->auth->addRole('user:123');
        $this->auth->addRole('team:456');
        $this->assertCount(3, $this->auth->getRoles());

        $this->auth->cleanRoles();
        $this->assertCount(0, $this->auth->getRoles());
        $this->assertFalse($this->auth->hasRole('any'));
    }

    public function testDisableAndEnable(): void
    {
        $this->assertTrue($this->auth->getStatus());

        $this->auth->disable();
        $this->assertFalse($this->auth->getStatus());

        $this->auth->enable();
        $this->assertTrue($this->auth->getStatus());
    }

    public function testDisabledAuthorizationBypassesAllChecks(): void
    {
        $this->auth->disable();
        $this->auth->cleanRoles();

        $input = new Input('read', ['user:999']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testSetDefaultStatus(): void
    {
        $this->auth->setDefaultStatus(false);
        $this->assertFalse($this->auth->getStatus());

        $this->auth->reset();
        $this->assertFalse($this->auth->getStatus());
    }

    public function testResetRestoresDefaultStatus(): void
    {
        $this->auth->setDefaultStatus(true);
        $this->auth->disable();
        $this->assertFalse($this->auth->getStatus());

        $this->auth->reset();
        $this->assertTrue($this->auth->getStatus());
    }

    public function testPermissionTypeMatchingRead(): void
    {
        $this->auth->addRole('user:123');

        $input = new Input('read', ['user:123']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testPermissionTypeMatchingCreate(): void
    {
        $this->auth->addRole('user:123');

        $input = new Input('create', ['user:123']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testPermissionTypeMatchingUpdate(): void
    {
        $this->auth->addRole('user:123');

        $input = new Input('update', ['user:123']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testPermissionTypeMatchingDelete(): void
    {
        $this->auth->addRole('user:123');

        $input = new Input('delete', ['user:123']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testPermissionTypeMatchingWrite(): void
    {
        $this->auth->addRole('user:123');

        $input = new Input('write', ['user:123']);
        $this->assertTrue($this->auth->isValid($input));
    }

    public function testHasRole(): void
    {
        $this->assertTrue($this->auth->hasRole('any'));
        $this->assertFalse($this->auth->hasRole('user:123'));

        $this->auth->addRole('user:123');
        $this->assertTrue($this->auth->hasRole('user:123'));
    }

    public function testIsArray(): void
    {
        $this->assertFalse($this->auth->isArray());
    }

    public function testGetType(): void
    {
        $this->assertEquals('array', $this->auth->getType());
    }

    public function testInputSettersAndGetters(): void
    {
        $input = new Input('read', ['user:123']);
        $this->assertEquals('read', $input->getAction());
        $this->assertEquals(['user:123'], $input->getPermissions());

        $input->setAction('write');
        $this->assertEquals('write', $input->getAction());

        $input->setPermissions(['team:456']);
        $this->assertEquals(['team:456'], $input->getPermissions());
    }

    public function testIsValidWithTeamDimensionRole(): void
    {
        $this->auth->addRole('team:abc/owner');

        $input = new Input('read', ['team:abc/owner']);
        $this->assertTrue($this->auth->isValid($input));

        $input = new Input('read', ['team:abc/member']);
        $this->assertFalse($this->auth->isValid($input));
    }

    public function testAddingDuplicateRoleDoesNotDuplicate(): void
    {
        $this->auth->addRole('user:123');
        $this->auth->addRole('user:123');

        $roles = array_filter($this->auth->getRoles(), fn ($r) => $r === 'user:123');
        $this->assertCount(1, $roles);
    }

    public function testRemovingNonExistentRoleDoesNotThrow(): void
    {
        $this->auth->removeRole('nonexistent');
        $this->assertFalse($this->auth->hasRole('nonexistent'));
    }

    public function testLabelRole(): void
    {
        $this->auth->addRole('label:vip');

        $input = new Input('read', ['label:vip']);
        $this->assertTrue($this->auth->isValid($input));

        $input = new Input('read', ['label:premium']);
        $this->assertFalse($this->auth->isValid($input));
    }

    public function testMemberRole(): void
    {
        $this->auth->addRole('member:abc123');

        $input = new Input('read', ['member:abc123']);
        $this->assertTrue($this->auth->isValid($input));

        $input = new Input('read', ['member:def456']);
        $this->assertFalse($this->auth->isValid($input));
    }
}
