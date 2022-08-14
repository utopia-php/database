<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Document;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Permissions;
use PHPUnit\Framework\TestCase;

class PermissionsTest extends TestCase
{

    public function setUp(): void
    {

    }

    public function tearDown(): void
    {
    }

    /**
     * @throws \Exception
     */
    public function testSingleMethodSingleValue()
    {
        $object = new Permissions();

        $document = new Document([
            '$id' => uniqid(),
            '$collection' => uniqid(),
            '$permissions' => [Permission::create(Role::any())],
        ]);
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::create(Role::users())];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::create(Role::user('123abc'))];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::create(Role::team('123abc'))];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::create(Role::team('123abc', 'edit'))];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = ['create("member:123abc")'];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::create(Role::guests())];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = ['create("status:verified")'];
        $this->assertTrue($object->isValid($document->getPermissions()));

        $document['$permissions'] = [Permission::read(Role::any())];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::read(Role::users())];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::read(Role::user('123abc'))];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::read(Role::team('123abc'))];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::read(Role::team('123abc', 'viewer'))];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = ['read("member:123abc")'];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::read(Role::guests())];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = ['read("status:verified")'];
        $this->assertTrue($object->isValid($document->getPermissions()));

        $document['$permissions'] = [Permission::update(Role::any())];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::update(Role::users())];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::update(Role::user('123abc'))];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::update(Role::team('123abc'))];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::update(Role::team('123abc', 'edit'))];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = ['update("member:123abc")'];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::update(Role::guests())];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = ['update("status:verified")'];
        $this->assertTrue($object->isValid($document->getPermissions()));

        $document['$permissions'] = [Permission::delete(Role::any())];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::delete(Role::users())];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::delete(Role::user('123abc'))];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::delete(Role::team('123abc'))];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::delete(Role::team('123abc', 'edit'))];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = ['delete("member:123abc")'];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [Permission::delete(Role::guests())];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = ['delete("status:verified")'];
        $this->assertTrue($object->isValid($document->getPermissions()));
    }

    public function testMultipleMethodSingleValue()
    {
        $object = new Permissions();

        $document = new Document([
            '$id' => uniqid(),
            '$collection' => uniqid(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
            ],
        ]);
        $this->assertTrue($object->isValid($document->getPermissions()));

        $document['$permissions'] = [
            Permission::read(Role::users()),
            Permission::create(Role::users()),
            Permission::update(Role::users()),
        ];
        $this->assertTrue($object->isValid($document->getPermissions()));

        $document['$permissions'] = [
            Permission::read(Role::user('123abc')),
            Permission::create(Role::user('123abc')),
            Permission::update(Role::user('123abc'))
        ];
        $this->assertTrue($object->isValid($document->getPermissions()));

        $document['$permissions'] = [
            Permission::read(Role::team('123abc')),
            Permission::create(Role::team('123abc')),
            Permission::update(Role::team('123abc'))
        ];
        $this->assertTrue($object->isValid($document->getPermissions()));

        $document['$permissions'] = [
            Permission::read(Role::team('123abc', 'viewer')),
            Permission::create(Role::team('123abc', 'viewer')),
            Permission::update(Role::team('123abc', 'viewer'))
        ];
        $this->assertTrue($object->isValid($document->getPermissions()));

        $document['$permissions'] = [
            Permission::read(Role::guests()),
            Permission::create(Role::guests()),
            Permission::update(Role::guests()),
        ];
        $this->assertTrue($object->isValid($document->getPermissions()));

        $document['$permissions'] = [
            Permission::read(Role::status('verified')),
            Permission::create(Role::status('verified')),
            Permission::update(Role::status('verified')),
        ];
        $this->assertTrue($object->isValid($document->getPermissions()));
    }

    public function testMultipleMethodMultipleValues()
    {
        $object = new Permissions();

        $document = new Document([
            '$id' => uniqid(),
            '$collection' => uniqid(),
            '$permissions' => [
                Permission::read(Role::users()),
                Permission::create(Role::user('123abc')),
                Permission::create(Role::team('123abc')),
                Permission::update(Role::user('123abc')),
                Permission::update(Role::team('123abc')),
                Permission::delete(Role::user('123abc')),
            ],
        ]);
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [
            Permission::read(Role::user('123abc')),
            Permission::read(Role::team('123abc')),
            Permission::create(Role::user('123abc')),
            Permission::create(Role::team('123abc')),
            Permission::update(Role::user('123abc')),
            Permission::update(Role::team('123abc')),
            Permission::delete(Role::user('123abc'))
        ];
        $this->assertTrue($object->isValid($document->getPermissions()));
        $document['$permissions'] = [
            Permission::read(Role::any()),
            Permission::create(Role::guests()),
            Permission::update(Role::team('123abc', 'edit')),
            Permission::delete(Role::team('123abc', 'edit'))
        ];
        $this->assertTrue($object->isValid($document->getPermissions()));
    }

    public function testInvalidPermissions()
    {
        $object = new Permissions();

        $this->assertFalse($object->isValid(Permission::create(Role::any())));
        $this->assertEquals('Permissions must be an array of strings.', $object->getDescription());
        $this->assertFalse($object->isValid(false));
        $this->assertEquals('Permissions must be an array of strings.', $object->getDescription());
        $this->assertFalse($object->isValid(1.5));
        $this->assertEquals('Permissions must be an array of strings.', $object->getDescription());

        // Permissions must be of type string
        $this->assertFalse($object->isValid([0, 1.5]));
        $this->assertEquals('Permission must be of type string.', $object->getDescription());
        $this->assertFalse($object->isValid([false, []]));
        $this->assertEquals('Permission must be of type string.', $object->getDescription());
        $this->assertFalse($object->isValid([['a']]));
        $this->assertEquals('Permission must be of type string.', $object->getDescription());

        // Wildcard character unsupported
        $this->assertFalse($object->isValid(['*']));
        $this->assertEquals('Wildcard permission "*" has been replaced. Use "any" instead.', $object->getDescription());

        // Role prefix values unsupported without method
        $this->assertFalse($object->isValid(['role:all']));

        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "role:all".', $object->getDescription());
        $this->assertFalse($object->isValid(['role:guest']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "role:guest".', $object->getDescription());
        $this->assertFalse($object->isValid(['role:member']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "role:member".', $object->getDescription());

        // Role prefix values deprecated
        $this->assertTrue($object->isValid(['read("role:all")']));
        $this->assertEquals('Permissions using the "role:" prefix have been deprecated. Use "users", "guests", or "any" instead.', $object->getDescription());
        $this->assertTrue($object->isValid(['create("role:guest")']));
        $this->assertEquals('Permissions using the "role:" prefix have been deprecated. Use "users", "guests", or "any" instead.', $object->getDescription());
        $this->assertTrue($object->isValid(['update("role:member")']));
        $this->assertEquals('Permissions using the "role:" prefix have been deprecated. Use "users", "guests", or "any" instead.', $object->getDescription());

        // Only contains a single ':'
        $this->assertFalse($object->isValid(['user1234']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "user1234".', $object->getDescription());
        $this->assertFalse($object->isValid(['user::1234']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "user::1234".', $object->getDescription());
        $this->assertFalse($object->isValid(['user:123:4']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "user:123:4".', $object->getDescription());

        // Split role into format {$type}:{$value}
        // Permission must have value
        $this->assertFalse($object->isValid(['read("member:")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("member:")".', $object->getDescription());
        $this->assertFalse($object->isValid(['read("user:")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("user:")".', $object->getDescription());
        $this->assertFalse($object->isValid(['read("team:")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("team:")".', $object->getDescription());

        // Permission role:$value must be one of: all, guest, member
        $this->assertFalse($object->isValid(['read("anyy")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("anyy")".', $object->getDescription());
        $this->assertFalse($object->isValid(['read("gguest")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("gguest")".', $object->getDescription());
        $this->assertFalse($object->isValid(['read("memer:123abc")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("memer:123abc")".', $object->getDescription());

        // team:$value, member:$value and user:$value must have valid Key for $value
        // No leading special chars
        $this->assertFalse($object->isValid(['read("member:_1234")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("member:_1234")".', $object->getDescription());
        $this->assertFalse($object->isValid(['read("member:-1234")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("member:-1234")".', $object->getDescription());
        $this->assertFalse($object->isValid(['read("member:.1234")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("member:.1234")".', $object->getDescription());

        // No unsupported special characters
        $this->assertFalse($object->isValid(['create("member:12$4")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "create("member:12$4")".', $object->getDescription());
        $this->assertFalse($object->isValid([Permission::create(Role::user('12&4'))]));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "create("user:12&4")".', $object->getDescription());
        $this->assertFalse($object->isValid(['create("member:ab(124")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "create("member:ab(124")".', $object->getDescription());

        // Shorter than 36 chars
        $this->assertTrue($object->isValid([Permission::read(Role::user('aaaaaaaabbbbbbbbccccccccddddddddeeee'))]));
        $this->assertFalse($object->isValid([Permission::read(Role::user('aaaaaaaabbbbbbbbccccccccddddddddeeeee'))]));
        $this->assertEquals("Identifier must be a valid key: Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can't start with a special char", $object->getDescription());

        // Permission role must begin with one of: member, role, team, user
        $this->assertFalse($object->isValid(['update("memmber:1234")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "update("memmber:1234")".', $object->getDescription());
        $this->assertFalse($object->isValid(['update("tteam:1234")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "update("tteam:1234")".', $object->getDescription());
        $this->assertFalse($object->isValid(['update("userr:1234")']));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "update("userr:1234")".', $object->getDescription());

        // Team permission
        $this->assertFalse($object->isValid([Permission::read(Role::team('_abcd'))]));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("team:_abcd")".', $object->getDescription());
        $this->assertFalse($object->isValid([Permission::read(Role::team('abcd/'))]));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("team:abcd/")".', $object->getDescription());
        $this->assertFalse($object->isValid([Permission::read(Role::team('','abcd'))]));
        $this->assertEquals('Identifier must not be empty.', $object->getDescription());
        $this->assertFalse($object->isValid([Permission::read(Role::team('abcd', '/efgh'))]));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("team:abcd//efgh")".', $object->getDescription());
        $this->assertFalse($object->isValid([Permission::read(Role::team('abcd', 'e/fgh'))]));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("team:abcd/e/fgh")".', $object->getDescription());
        $this->assertFalse($object->isValid([Permission::read(Role::team('ab&cd3', 'efgh'))]));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("team:ab&cd3/efgh")".', $object->getDescription());
        $this->assertFalse($object->isValid([Permission::read(Role::team('abcd', 'ef*gh'))]));
        $this->assertEquals('Must be of the form "permission("role:id/dimension")", got "read("team:abcd/ef*gh")".', $object->getDescription());

        // Permission-list length must be valid
        $object = new Permissions(100);
        $permissions = \array_fill(0, 100, Permission::read(Role::any()));
        $this->assertTrue($object->isValid($permissions));
        $permissions[] = Permission::read(Role::any());
        $this->assertFalse($object->isValid($permissions));
        $this->assertEquals('You can only provide up to 100 permissions.', $object->getDescription());
    }
}