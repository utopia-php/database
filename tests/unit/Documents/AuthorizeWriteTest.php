<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Permission;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

/**
 * Database::authorizeCreate() and authorizeUpdate() run the permission checks of a later write, nested related
 * documents included, without writing.
 */
final class AuthorizeWriteTest extends TestCase
{
    private const string USER = 'u1';

    private const string NO_CREATE = "No permissions provided for action 'create'";

    private const string NO_READ = "No permissions provided for action 'read'";

    private const string NO_UPDATE = "No permissions provided for action 'update'";

    private Authorization $authorization;

    private Database $database;

    public function testACreateNeedsCreateOnTheCollection(): void
    {
        $this->seed(parents: [Permission::read(Role::any())]);

        $this->assertSame(self::NO_CREATE, $this->refusal(fn () => $this->database->authorizeCreate('parents', new Document(['$id' => 'parent']))));
    }

    public function testANewNestedDocumentNeedsCreateOnItsCollection(): void
    {
        $this->seed(children: [Permission::read(Role::users())]);
        $parent = new Document(['$id' => 'parent', 'children' => [['$id' => 'new', 'name' => 'New']]]);

        $this->assertSame(self::NO_CREATE, $this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)));

        $this->seed(children: [Permission::create(Role::users())]);
        $this->assertNull($this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)));
    }

    public function testALinkTwoLevelsDownNeedsUpdateOnTheLinkedDocument(): void
    {
        $parent = new Document(['$id' => 'parent', 'children' => [['$id' => 'new', 'name' => 'New', 'toys' => ['toy']]]]);

        $this->seed(toys: [Permission::read(Role::users())]);
        $this->assertSame(self::NO_UPDATE, $this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)));

        $this->seed(toys: [Permission::create(Role::users())]);
        $this->assertSame(self::NO_READ, $this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)), 'A toy the caller cannot read is refused');

        $this->seed(toys: [Permission::read(Role::users()), Permission::update(Role::users())]);
        $this->assertNull($this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)));
    }

    public function testAStoredNestedDocumentTheCallerCannotReadIsRefusedRatherThanCreated(): void
    {
        $this->seed(children: [Permission::create(Role::users())]);
        $parent = new Document(['$id' => 'parent', 'children' => [['$id' => 'stored', 'name' => 'Stored']]]);

        $this->assertSame(self::NO_UPDATE, $this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)));
    }

    public function testAnUpdateNeedsUpdateOnTheDocument(): void
    {
        $this->seed(parents: [Permission::read(Role::any())], documentSecurity: true);
        $own = $this->stored('parents', 'own', [Permission::update(Role::user(self::USER))]);
        $other = $this->stored('parents', 'other', [Permission::read(Role::any())]);

        $this->assertNull($this->refusal(fn () => $this->database->authorizeUpdate('parents', $own, new Document(['name' => 'Changed']))));
        $this->assertSame(self::NO_UPDATE, $this->refusal(fn () => $this->database->authorizeUpdate('parents', $other, new Document(['name' => 'Changed']))));
    }

    public function testAnUpdateChecksOnlyTheDocumentsItLinksAnew(): void
    {
        $this->seed(children: [Permission::read(Role::users())]);
        $this->stored('children', 'second', []);
        $old = $this->authorization->skip(function (): Document {
            $this->database->createDocument('parents', new Document(['$id' => 'parent', 'children' => ['stored']]));

            return $this->database->getDocument('parents', 'parent');
        });

        $this->assertNull($this->refusal(fn () => $this->database->authorizeUpdate('parents', $old, new Document(['children' => ['stored']]))), 'A document the parent already links is not linked again');
        $this->assertSame(self::NO_UPDATE, $this->refusal(fn () => $this->database->authorizeUpdate('parents', $old, new Document(['children' => ['stored', 'second']]))));
    }

    public function testAnApiKeyPassesEveryCheck(): void
    {
        $this->seed(parents: [], children: [], toys: []);
        $parent = new Document(['$id' => 'parent', 'children' => ['stored', ['$id' => 'new', 'toys' => ['toy']]]]);

        $this->assertNull($this->authorization->skip(fn () => $this->refusal(fn () => $this->database->authorizeCreate('parents', $parent))));
    }

    public function testWithRelationshipsSkippedOnlyTheDocumentIsChecked(): void
    {
        $this->seed(children: []);
        $parent = new Document(['$id' => 'parent', 'children' => ['stored']]);

        $this->assertSame(self::NO_READ, $this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)));
        $this->assertNull($this->refusal(fn () => $this->database->skipRelationships(fn () => $this->database->authorizeCreate('parents', $parent))));
    }

    public function testAnUnknownCollectionIsNotFound(): void
    {
        $this->seed();

        $this->expectException(NotFoundException::class);
        $this->database->authorizeCreate('missing', new Document());
    }

    public function testTheChecksWriteNothing(): void
    {
        $this->seed();
        $parent = new Document(['$id' => 'parent', 'children' => ['stored', ['$id' => 'new', 'name' => 'New', 'toys' => ['toy']]]]);

        $this->assertNull($this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)));

        $this->authorization->skip(function (): void {
            $this->assertTrue($this->database->getDocument('parents', 'parent')->isEmpty());
            $this->assertTrue($this->database->getDocument('children', 'new')->isEmpty());
            $this->assertNull($this->database->getDocument('children', 'stored')->getAttribute('parent'));
            $this->assertSame([], $this->database->getDocument('toys', 'toy')->getAttribute('children'));
        });
    }

    /**
     * Each collection grants users create, read and update unless given its permissions.
     *
     * @param  list<string>|null  $parents
     * @param  list<string>|null  $children
     * @param  list<string>|null  $toys
     */
    private function seed(
        ?array $parents = null,
        ?array $children = null,
        ?array $toys = null,
        bool $documentSecurity = false,
    ): void {
        $everything = [Permission::create(Role::users()), Permission::read(Role::users()), Permission::update(Role::users())];
        $this->authorization = new Authorization();
        $this->authorization->addRole(Role::any()->toString());
        $this->authorization->addRole(Role::users()->toString());
        $this->authorization->addRole(Role::user(self::USER)->toString());
        $this->database = (new Database(new Memory(), new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('authorize_write')
            ->setNamespace('authorize_write_'.\uniqid());
        $this->database->addHook(new Permissions());
        $this->database->addHook(new Relationships());

        $this->authorization->skip(function () use ($parents, $children, $toys, $documentSecurity, $everything): void {
            $this->database->create();
            foreach (['parents' => $parents, 'children' => $children, 'toys' => $toys] as $collection => $permissions) {
                $this->database->createCollection(Collection::create(
                    $collection,
                    attributes: [Attribute::string('name', size: 64)],
                    permissions: $permissions ?? $everything,
                    documentSecurity: $collection === 'parents' && $documentSecurity,
                ));
            }
            $this->database->createRelationship('parents', Relationship::oneToMany('children', 'children', twoWay: true, twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull));
            $this->database->createRelationship('children', Relationship::manyToMany('toys', 'toys', twoWay: true, twoWayKey: 'children', onDelete: RelationshipDeleteAction::SetNull));
            $this->database->createDocument('children', new Document(['$id' => 'stored', 'name' => 'Stored']));
            $this->database->createDocument('toys', new Document(['$id' => 'toy', 'name' => 'Toy']));
        });
    }

    /**
     * @param  list<string>  $permissions
     */
    private function stored(string $collection, string $id, array $permissions): Document
    {
        return $this->authorization->skip(fn (): Document => $this->database->createDocument($collection, new Document([
            '$id' => $id,
            '$permissions' => $permissions,
        ])));
    }

    private function refusal(callable $check): ?string
    {
        try {
            $check();
        } catch (AuthorizationException $error) {
            return $error->getMessage();
        }

        return null;
    }
}
