<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Id;
use Utopia\Database\Permission;
use Utopia\Database\PermissionType;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Authorization\Input;

class AuthorizationTest extends TestCase
{
    protected Authorization $authorization;

    #[\Override]
    protected function setUp(): void
    {
        $this->authorization = new Authorization();
    }

    #[\Override]
    protected function tearDown(): void
    {
    }

    public function test_values(): void
    {
        $this->authorization->addRole(Role::any()->toString());

        $document = new Document([
            '$id' => Id::unique(),
            '$collection' => Id::unique(),
            '$permissions' => [
                Permission::read(Role::user(Id::custom('123'))),
                Permission::read(Role::team(Id::custom('123'))),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]);

        $object = $this->authorization;

        $this->assertEquals($object->isValid(new Input(PermissionType::Read, $document->getPermissionsByType(PermissionType::Read))), false);
        $this->assertEquals($object->isValid(new Input(PermissionType::Read, [])), false);
        $this->assertEquals($object->getDescription(), 'No permissions provided for action \'read\'');

        $this->authorization->addRole(Role::user('456')->toString());
        $this->authorization->addRole(Role::user('123')->toString());

        $this->assertEquals($this->authorization->hasRole(Role::user('456')->toString()), true);
        $this->assertEquals($this->authorization->hasRole(Role::user('457')->toString()), false);
        $this->assertEquals($this->authorization->hasRole(''), false);
        $this->assertEquals($this->authorization->hasRole(Role::any()->toString()), true);

        $this->assertEquals($object->isValid(new Input(PermissionType::Read, $document->getPermissionsByType(PermissionType::Read))), true);

        $this->authorization->cleanRoles();

        $this->assertEquals($object->isValid(new Input(PermissionType::Read, $document->getPermissionsByType(PermissionType::Read))), false);

        $this->authorization->addRole(Role::team('123')->toString());

        $this->assertEquals($object->isValid(new Input(PermissionType::Read, $document->getPermissionsByType(PermissionType::Read))), true);

        $this->authorization->cleanRoles();
        $this->authorization->disable();

        $this->assertEquals($object->isValid(new Input(PermissionType::Read, $document->getPermissionsByType(PermissionType::Read))), true);

        $this->authorization->reset();

        $this->assertEquals($object->isValid(new Input(PermissionType::Read, $document->getPermissionsByType(PermissionType::Read))), false);

        $this->authorization = (new Authorization(defaultStatus: false))->cleanRoles();
        $object = $this->authorization;
        $this->assertFalse($object->getStatus());
        $this->authorization->disable();

        $this->assertEquals($object->isValid(new Input(PermissionType::Read, $document->getPermissionsByType(PermissionType::Read))), true);

        $this->authorization->reset();

        $this->assertEquals($object->isValid(new Input(PermissionType::Read, $document->getPermissionsByType(PermissionType::Read))), true);

        $this->authorization->enable();

        $this->assertEquals($object->isValid(new Input(PermissionType::Read, $document->getPermissionsByType(PermissionType::Read))), false);

        $this->authorization->addRole('textX');

        $this->assertContains('textX', $this->authorization->getRoles());

        $this->authorization->removeRole('textX');

        $this->assertNotContains('textX', $this->authorization->getRoles());

        // Test skip method
        $this->assertEquals($object->isValid(new Input(PermissionType::Read, $document->getPermissionsByType(PermissionType::Read))), false);
        $this->assertEquals($this->authorization->skip(function () use ($object, $document) {
            return $object->isValid(new Input(PermissionType::Read, $document->getPermissionsByType(PermissionType::Read)));
        }), true);
    }

    public function test_nested_skips(): void
    {
        $this->assertEquals(true, $this->authorization->getStatus());

        $this->authorization->skip(function () {
            $this->assertEquals(false, $this->authorization->getStatus());

            $this->authorization->skip(function () {
                $this->assertEquals(false, $this->authorization->getStatus());

                $this->authorization->skip(function () {
                    $this->assertEquals(false, $this->authorization->getStatus());
                });

                $this->assertEquals(false, $this->authorization->getStatus());
            });

            $this->assertEquals(false, $this->authorization->getStatus());
        });

        $this->assertEquals(true, $this->authorization->getStatus());
    }

    public function test_custom_action_granted(): void
    {
        $this->authorization->addRole(Role::user(Id::custom('123'))->toString());

        $this->assertTrue($this->authorization->isValid(new Input('execute', [Role::user(Id::custom('123'))->toString()])));
        $this->assertTrue($this->authorization->isValid(new Input('subscribe', [Role::any()->toString()])));
    }

    public function test_custom_action_denied(): void
    {
        $input = new Input('execute', [Role::user(Id::custom('456'))->toString()]);

        $this->assertSame('execute', $input->getAction());
        $this->assertFalse($this->authorization->isValid($input));
        $this->assertStringContainsString('Missing "execute" permission', $this->authorization->getDescription());

        $this->assertFalse($this->authorization->isValid(new Input('execute', [])));
        $this->assertSame("No permissions provided for action 'execute'", $this->authorization->getDescription());
    }

    public function test_set_action_accepts_custom_string_and_enum(): void
    {
        $input = new Input(PermissionType::Read, []);

        $this->assertSame('subscribe', $input->setAction('subscribe')->getAction());
        $this->assertSame('update', $input->setAction(PermissionType::Update)->getAction());
    }
}
