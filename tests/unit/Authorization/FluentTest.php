<?php

namespace Tests\Unit\Authorization;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Helpers\Role;
use Utopia\Database\PermissionType;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Authorization\Input;

final class FluentTest extends TestCase
{
    public function testRoleSettersChainOnTheSameInstance(): void
    {
        $authorization = new Authorization();
        $user = Role::user('123')->toString();
        $team = Role::team('abc')->toString();

        $chained = $authorization
            ->cleanRoles()
            ->addRole($user)
            ->addRole($team)
            ->removeRole($team);

        $this->assertSame($authorization, $chained);
        $this->assertSame([$user], $authorization->getRoles());
    }

    public function testStatusSettersChainOnTheSameInstance(): void
    {
        $authorization = new Authorization();

        $this->assertSame($authorization, $authorization->disable());
        $this->assertFalse($authorization->getStatus());

        $this->assertSame($authorization, $authorization->enable());
        $this->assertTrue($authorization->getStatus());

        $this->assertSame($authorization, $authorization->setStatus(false));
        $this->assertFalse($authorization->getStatus());
    }

    public function testTheDefaultStatusIsTrue(): void
    {
        $authorization = new Authorization();

        $this->assertTrue($authorization->getStatus());

        $authorization->disable()->reset();
        $this->assertTrue($authorization->getStatus());
    }

    public function testADisabledDefaultStatusSkipsChecksUntilEnabled(): void
    {
        $authorization = (new Authorization(defaultStatus: false))->cleanRoles();
        $input = new Input(PermissionType::Read, [Role::user('123')->toString()]);

        $this->assertFalse($authorization->getStatus());
        $this->assertTrue($authorization->isValid($input));

        $authorization->enable();
        $this->assertFalse($authorization->isValid($input));

        $authorization->reset();
        $this->assertTrue($authorization->isValid($input));
    }

    public function testSkipLiftsTheCheckOnlyInsideTheCallback(): void
    {
        $authorization = (new Authorization())->cleanRoles();
        $input = new Input(PermissionType::Delete, [Role::user('123')->toString()]);

        $this->assertTrue($authorization->skip(static fn (): bool => $authorization->isValid($input)));
        $this->assertFalse($authorization->isValid($input));
    }

    public function testInputTakesThePermissionType(): void
    {
        $input = new Input(PermissionType::Update, ['any']);

        $this->assertSame(PermissionType::Update->value, $input->getAction());
        $this->assertSame(['any'], $input->getPermissions());

        $this->assertSame($input, $input->setAction(PermissionType::Create)->setPermissions(['users']));
        $this->assertSame(PermissionType::Create->value, $input->getAction());
        $this->assertSame(['users'], $input->getPermissions());
    }

    public function testAMissingPermissionNamesTheAction(): void
    {
        $authorization = (new Authorization())->cleanRoles();

        $this->assertFalse($authorization->isValid(new Input(PermissionType::Update, [Role::user('123')->toString()])));
        $this->assertStringContainsString('"update"', $authorization->getDescription());
    }
}
