<?php

namespace Tests\E2E\Adapter\Scopes\Relationships;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Id;
use Utopia\Database\Permission;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\Role;

trait NestedPreparationTests
{
    public function testNestedPreparationGeneratesPlaceholderIdsOnCreate(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, $children, $owners] = $this->createNestedPreparationCollections($database);

        $created = $database->withReplacePlaceholders(true, fn (): Document => $database->createDocument($parents, new Document([
            Document::ID => Id::PLACEHOLDER,
            'name' => 'parent',
            'children' => [
                new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'placeholder child']),
                new Document(['name' => 'empty child']),
                new Document([Document::ID => 'explicit', 'name' => 'explicit child']),
            ],
            'owner' => new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'owner']),
        ])));

        $this->assertNotSame(Id::PLACEHOLDER, $created->getId());
        $stored = $database->getDocument($parents, $created->getId());
        $this->assertSame('parent', $stored->getAttribute('name'));
        $childIds = $this->nestedPreparationIds($stored->getAttribute('children'));
        $this->assertCount(3, $childIds);
        $this->assertContains('explicit', $childIds);
        $this->assertNotContains(Id::PLACEHOLDER, $childIds);
        $owner = $stored->getAttribute('owner');
        $this->assertInstanceOf(Document::class, $owner);
        $this->assertNotSame(Id::PLACEHOLDER, $owner->getId());
        $this->assertTrue($database->getDocument($children, Id::PLACEHOLDER)->isEmpty());
        $this->assertTrue($database->getDocument($owners, Id::PLACEHOLDER)->isEmpty());
        $this->assertFalse($database->isReplacingPlaceholders());
    }

    public function testNestedPreparationGeneratesPlaceholderIdsOnUpdateAndUpsert(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, $children] = $this->createNestedPreparationCollections($database);
        $database->createDocument($parents, new Document([Document::ID => 'updated', 'name' => 'updated']));

        $database->withReplacePlaceholders(true, function () use ($database, $parents): void {
            $database->updateDocument($parents, 'updated', new Document([
                'children' => [new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'updated child'])],
            ]));

            if ($database->getAdapter()->hasFeature(Feature\Upserts::class)) {
                $database->upsertDocument($parents, new Document([
                    Document::ID => 'upserted',
                    'name' => 'upserted',
                    'children' => [new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'upserted child'])],
                ]));
            }
        });

        $updated = $this->nestedPreparationIds($database->getDocument($parents, 'updated')->getAttribute('children'));
        $this->assertCount(1, $updated);
        $this->assertNotSame(Id::PLACEHOLDER, $updated[0]);
        $this->assertSame('updated child', $database->getDocument($children, $updated[0])->getAttribute('name'));
        $this->assertTrue($database->getDocument($children, Id::PLACEHOLDER)->isEmpty());

        if ($database->getAdapter()->hasFeature(Feature\Upserts::class)) {
            $upserted = $this->nestedPreparationIds($database->getDocument($parents, 'upserted')->getAttribute('children'));
            $this->assertCount(1, $upserted);
            $this->assertNotSame(Id::PLACEHOLDER, $upserted[0]);
            $this->assertNotSame($updated[0], $upserted[0]);
        }
    }

    public function testNestedPreparationKeepsAStringPlaceholderAsAReference(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, , $owners] = $this->createNestedPreparationCollections($database);
        $database->createDocument($owners, new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'literal']));

        $database->withReplacePlaceholders(true, fn (): Document => $database->createDocument($parents, new Document([
            Document::ID => 'linked',
            'name' => 'linked',
            'owner' => Id::PLACEHOLDER,
        ])));

        $owner = $database->getDocument($parents, 'linked')->getAttribute('owner');
        $this->assertInstanceOf(Document::class, $owner);
        $this->assertSame(Id::PLACEHOLDER, $owner->getId());
        $this->assertSame('literal', $owner->getAttribute('name'));
    }

    public function testNestedPreparationChecksGrants(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, $children] = $this->createNestedPreparationCollections($database);
        $unheld = [Permission::read(Role::user('nestedPreparationUnheld'))];
        $refusal = $this->nestedPreparationRefusal(fn () => $database->withCheckGrants(true, fn (): Document => $database->createDocument($parents, $this->nestedPreparationParent('refused', $unheld))));
        $this->assertSame($this->nestedPreparationUnheldRefusal($database), $refusal);
        $this->assertTrue($database->getDocument($parents, 'refused')->isEmpty());
        $this->assertSame([], $database->find($children));

        $database->withCheckGrants(true, fn (): Document => $database->createDocument($parents, $this->nestedPreparationParent('held', [Permission::read(Role::any())])));
        $database->withCheckGrants(true, fn (): Document => $database->getAuthorization()->skip(fn (): Document => $database->createDocument($parents, $this->nestedPreparationParent('privileged', $unheld))));
        $database->createDocument($parents, $this->nestedPreparationParent('unchecked', $unheld));

        $this->assertCount(1, $this->nestedPreparationIds($database->getDocument($parents, 'held')->getAttribute('children')));
        $this->assertFalse($database->getAuthorization()->skip(fn (): Document => $database->getDocument($parents, 'privileged'))->isEmpty());
        $this->assertFalse($database->getAuthorization()->skip(fn (): Document => $database->getDocument($parents, 'unchecked'))->isEmpty());

        $database->createDocument($parents, new Document([Document::ID => 'update target', 'name' => 'update target']));
        $refusal = $this->nestedPreparationRefusal(fn () => $database->withCheckGrants(true, fn (): Document => $database->updateDocument($parents, 'update target', new Document([
            'children' => [new Document(['name' => 'update refused', Document::PERMISSIONS => $unheld])],
        ]))));
        $this->assertSame($this->nestedPreparationUnheldRefusal($database), $refusal);
        $this->assertSame([], $this->nestedPreparationIds($database->getDocument($parents, 'update target')->getAttribute('children')));
    }

    public function testNestedPreparationRefusesAnUnheldPlaceholderInWritesAndPreChecks(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, $children] = $this->createNestedPreparationCollections($database);
        $parent = static fn (string $id, string $role): Document => new Document([
            Document::ID => $id,
            'name' => $id,
            'children' => [new Document([Document::ID => Id::PLACEHOLDER, 'name' => $id, Document::PERMISSIONS => [Permission::read(Role::parse($role))]])],
        ]);
        $prepared = static fn (callable $callback): mixed => $database->withReplacePlaceholders(true, fn (): mixed => $database->withCheckGrants(true, $callback));

        $this->assertSame($this->nestedPreparationUnheldRefusal($database), $this->nestedPreparationRefusal(fn () => $prepared(fn () => $database->authorizeCreate($parents, $parent('checked', 'user:nestedPreparationUnheld')))));
        $this->assertSame($this->nestedPreparationUnheldRefusal($database), $this->nestedPreparationRefusal(fn () => $prepared(fn (): Document => $database->createDocument($parents, $parent('refused', 'user:nestedPreparationUnheld')))));
        $this->assertTrue($database->getDocument($parents, 'refused')->isEmpty());
        $this->assertSame([], $database->find($children));

        $this->assertNull($this->nestedPreparationRefusal(fn () => $prepared(fn () => $database->authorizeCreate($parents, $parent('created', 'any')))));
        $prepared(fn (): Document => $database->createDocument($parents, $parent('created', 'any')));
        $created = $this->nestedPreparationIds($database->getDocument($parents, 'created')->getAttribute('children'));
        $this->assertCount(1, $created);
        $this->assertNotSame(Id::PLACEHOLDER, $created[0]);
        $this->assertTrue($database->getDocument($children, Id::PLACEHOLDER)->isEmpty());
    }

    /**
     * @return array{string, string, string} The parent, child and owner collection IDs
     */
    private function createNestedPreparationCollections(Database $database): array
    {
        $parents = Id::unique();
        $children = Id::unique();
        $owners = Id::unique();
        $permissions = [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];

        foreach ([$parents, $children, $owners] as $collection) {
            $database->createCollection(Collection::create(
                id: $collection,
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: $permissions,
                documentSecurity: false,
            ));
        }

        $database->createRelationship($parents, Relationship::oneToMany(
            relatedCollection: $children,
            key: 'children',
            twoWay: true,
            twoWayKey: 'parent',
            onDelete: RelationshipDeleteAction::SetNull,
        ));
        $database->createRelationship($parents, Relationship::manyToOne(
            relatedCollection: $owners,
            key: 'owner',
            onDelete: RelationshipDeleteAction::SetNull,
        ));

        return [$parents, $children, $owners];
    }

    /**
     * @param  list<string>  $permissions  The permissions of the parent's new child
     */
    private function nestedPreparationParent(string $id, array $permissions): Document
    {
        return new Document([
            Document::ID => $id,
            'name' => $id,
            'children' => [new Document(['name' => $id, Document::PERMISSIONS => $permissions])],
        ]);
    }

    /**
     * @return list<string>
     */
    private function nestedPreparationIds(mixed $value): array
    {
        $values = \is_array($value) ? $value : ($value === null ? [] : [$value]);

        return \array_values(\array_map(static fn (mixed $item): string => $item instanceof Document ? $item->getId() : (\is_string($item) ? $item : ''), $values));
    }

    private function nestedPreparationUnheldRefusal(Database $database): string
    {
        return 'Permissions must be one of: ('.\implode(', ', $database->getAuthorization()->getRoles()).')';
    }

    private function nestedPreparationRefusal(callable $write): ?string
    {
        try {
            $write();
        } catch (AuthorizationException $error) {
            return $error->getMessage();
        }

        return null;
    }
}
