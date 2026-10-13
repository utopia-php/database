<?php

namespace Tests\E2E\Adapter\Scopes\Relationships;

use Closure;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Hook\Nested;
use Utopia\Database\Id;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\Role;

trait NestedPreparationTests
{
    private const string NESTED_PREPARATION_MARKER = 'marker';

    /**
     * @var list<array{string, string}> The collection and ID of each related document the nested hook was given
     */
    private array $nestedPreparationSeen = [];

    public function testNestedHookRewritesRelatedDocumentsOnCreate(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, $children, $owners, $profiles, $tags] = $this->createNestedPreparationHookCollections($database);
        $hook = $this->nestedPreparationHook();

        $database->addHook($hook);
        try {
            $database->createDocument($parents, new Document([
                Document::ID => 'created',
                'name' => 'created',
                'children' => [
                    new Document([
                        Document::ID => self::NESTED_PREPARATION_MARKER,
                        'name' => 'marked child',
                        'tags' => [new Document([Document::ID => self::NESTED_PREPARATION_MARKER, 'name' => 'marked tag'])],
                    ]),
                    new Document(['name' => 'empty child']),
                    new Document([Document::ID => 'explicit', 'name' => 'explicit child']),
                ],
                'owner' => new Document([Document::ID => self::NESTED_PREPARATION_MARKER, 'name' => 'marked owner']),
                'profile' => new Document([Document::ID => self::NESTED_PREPARATION_MARKER, 'name' => 'marked profile']),
            ]));
        } finally {
            $database->removeHook($hook);
        }

        $stored = $database->getDocument($parents, 'created');
        $childIds = $this->nestedPreparationIds($stored->getAttribute('children'));
        $this->assertCount(3, $childIds);
        $this->assertContains('explicit', $childIds);
        $this->assertNotContains(self::NESTED_PREPARATION_MARKER, $childIds);
        $child = $this->nestedPreparationNamed($database, $children, 'marked child');
        $this->assertContains($child->getId(), $childIds);
        $tag = $this->nestedPreparationNamed($database, $tags, 'marked tag');
        $this->assertSame([$tag->getId()], $this->nestedPreparationIds($database->getDocument($children, $child->getId())->getAttribute('tags')));
        $owner = $this->nestedPreparationNamed($database, $owners, 'marked owner');
        $this->assertSame([$owner->getId()], $this->nestedPreparationIds($stored->getAttribute('owner')));
        $profile = $this->nestedPreparationNamed($database, $profiles, 'marked profile');
        $this->assertSame([$profile->getId()], $this->nestedPreparationIds($stored->getAttribute('profile')));
        $this->assertNothingStoredUnderTheNestedPreparationMarker($database, [$children, $owners, $profiles, $tags]);
        $this->assertEqualsCanonicalizing([
            [$children, self::NESTED_PREPARATION_MARKER],
            [$tags, self::NESTED_PREPARATION_MARKER],
            [$children, ''],
            [$children, 'explicit'],
            [$owners, self::NESTED_PREPARATION_MARKER],
            [$profiles, self::NESTED_PREPARATION_MARKER],
        ], $this->nestedPreparationSeen);
    }

    public function testNestedHookRewritesOnUpdateUpsertAndCreateDocuments(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, $children, $owners, $profiles, $tags] = $this->createNestedPreparationHookCollections($database);
        $database->createDocument($parents, new Document([Document::ID => 'updated', 'name' => 'updated']));
        $upserts = $database->getAdapter()->hasFeature(Feature\Upserts::class);
        $hook = $this->nestedPreparationHook();

        $database->addHook($hook);
        try {
            $database->updateDocument($parents, 'updated', new Document([
                'children' => [$this->nestedPreparationMarked('updated child', ['tags' => [$this->nestedPreparationMarked('updated tag')]])],
            ]));
            $database->createDocuments($parents, [
                new Document([Document::ID => 'batched', 'name' => 'batched', 'owner' => $this->nestedPreparationMarked('batched owner'), 'profile' => $this->nestedPreparationMarked('batched profile')]),
            ]);
            if ($upserts) {
                $database->upsertDocument($parents, new Document([
                    Document::ID => 'upserted',
                    'name' => 'upserted',
                    'children' => [$this->nestedPreparationMarked('upserted child')],
                    'profile' => $this->nestedPreparationMarked('upserted profile'),
                ]));
                $database->upsertDocuments($parents, [new Document([
                    Document::ID => 'upserted in bulk',
                    'name' => 'upserted in bulk',
                    'owner' => $this->nestedPreparationMarked('upserted owner'),
                ])]);
            }
        } finally {
            $database->removeHook($hook);
        }

        $updatedChild = $this->nestedPreparationNamed($database, $children, 'updated child');
        $this->assertSame([$updatedChild->getId()], $this->nestedPreparationIds($database->getDocument($parents, 'updated')->getAttribute('children')));
        $updatedTag = $this->nestedPreparationNamed($database, $tags, 'updated tag');
        $this->assertSame([$updatedTag->getId()], $this->nestedPreparationIds($database->getDocument($children, $updatedChild->getId())->getAttribute('tags')));
        $batched = $database->getDocument($parents, 'batched');
        $batchedOwner = $this->nestedPreparationNamed($database, $owners, 'batched owner');
        $batchedProfile = $this->nestedPreparationNamed($database, $profiles, 'batched profile');
        $this->assertSame([$batchedOwner->getId()], $this->nestedPreparationIds($batched->getAttribute('owner')));
        $this->assertSame([$batchedProfile->getId()], $this->nestedPreparationIds($batched->getAttribute('profile')));
        $generated = [$updatedChild->getId(), $updatedTag->getId(), $batchedOwner->getId(), $batchedProfile->getId()];

        if ($upserts) {
            $upserted = $database->getDocument($parents, 'upserted');
            $upsertedChild = $this->nestedPreparationNamed($database, $children, 'upserted child');
            $upsertedProfile = $this->nestedPreparationNamed($database, $profiles, 'upserted profile');
            $upsertedOwner = $this->nestedPreparationNamed($database, $owners, 'upserted owner');
            $this->assertSame([$upsertedChild->getId()], $this->nestedPreparationIds($upserted->getAttribute('children')));
            $this->assertSame([$upsertedProfile->getId()], $this->nestedPreparationIds($upserted->getAttribute('profile')));
            $this->assertSame([$upsertedOwner->getId()], $this->nestedPreparationIds($database->getDocument($parents, 'upserted in bulk')->getAttribute('owner')));
            \array_push($generated, $upsertedChild->getId(), $upsertedProfile->getId(), $upsertedOwner->getId());
        }

        $this->assertNotContains(self::NESTED_PREPARATION_MARKER, $generated);
        $this->assertCount(\count($generated), \array_unique($generated));
        $this->assertNothingStoredUnderTheNestedPreparationMarker($database, [$children, $owners, $profiles, $tags]);
        $marker = self::NESTED_PREPARATION_MARKER;
        $this->assertEqualsCanonicalizing([
            [$children, $marker],
            [$tags, $marker],
            [$owners, $marker],
            [$profiles, $marker],
            ...($upserts ? [[$children, $marker], [$profiles, $marker], [$owners, $marker]] : []),
        ], $this->nestedPreparationSeen);
    }

    public function testNestedHookRewritesFromTheChildSide(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, $children, $owners, $profiles, $tags] = $this->createNestedPreparationHookCollections($database);
        $marked = static fn (string $name, array $attributes = []): Document => new Document([Document::ID => self::NESTED_PREPARATION_MARKER, 'name' => $name, ...$attributes]);
        $hook = $this->nestedPreparationHook();

        $database->addHook($hook);
        try {
            $database->createDocument($children, new Document([
                Document::ID => 'child side',
                'name' => 'child side',
                'parent' => $this->nestedPreparationMarked('parent of a child', ['owner' => $this->nestedPreparationMarked('owner of a parent')]),
            ]));
            $database->createDocument($owners, new Document([Document::ID => 'owner side', 'name' => 'owner side', 'parents' => [$this->nestedPreparationMarked('parent of an owner')]]));
            $database->createDocument($profiles, new Document([Document::ID => 'profile side', 'name' => 'profile side', 'parent' => $this->nestedPreparationMarked('parent of a profile')]));
            $database->createDocument($tags, new Document([Document::ID => 'tag side', 'name' => 'tag side', 'children' => [$this->nestedPreparationMarked('child of a tag')]]));
        } finally {
            $database->removeHook($hook);
        }

        $parentOfAChild = $this->nestedPreparationNamed($database, $parents, 'parent of a child');
        $this->assertSame([$parentOfAChild->getId()], $this->nestedPreparationIds($database->getDocument($children, 'child side')->getAttribute('parent')));
        $ownerOfAParent = $this->nestedPreparationNamed($database, $owners, 'owner of a parent');
        $this->assertSame([$ownerOfAParent->getId()], $this->nestedPreparationIds($database->getDocument($parents, $parentOfAChild->getId())->getAttribute('owner')));
        $parentOfAnOwner = $this->nestedPreparationNamed($database, $parents, 'parent of an owner');
        $this->assertSame([$parentOfAnOwner->getId()], $this->nestedPreparationIds($database->getDocument($owners, 'owner side')->getAttribute('parents')));
        $parentOfAProfile = $this->nestedPreparationNamed($database, $parents, 'parent of a profile');
        $this->assertSame([$parentOfAProfile->getId()], $this->nestedPreparationIds($database->getDocument($profiles, 'profile side')->getAttribute('parent')));
        $childOfATag = $this->nestedPreparationNamed($database, $children, 'child of a tag');
        $this->assertSame([$childOfATag->getId()], $this->nestedPreparationIds($database->getDocument($tags, 'tag side')->getAttribute('children')));
        $this->assertNothingStoredUnderTheNestedPreparationMarker($database, [$parents, $children, $owners]);
        $marker = self::NESTED_PREPARATION_MARKER;
        $this->assertEqualsCanonicalizing([
            [$parents, $marker],
            [$owners, $marker],
            [$parents, $marker],
            [$parents, $marker],
            [$children, $marker],
        ], $this->nestedPreparationSeen);
    }

    public function testNestedHookKeepsAStringMarkerAsAReference(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, , $owners] = $this->createNestedPreparationHookCollections($database);
        $database->createDocument($owners, new Document([Document::ID => self::NESTED_PREPARATION_MARKER, 'name' => 'literal']));
        $hook = $this->nestedPreparationHook();

        $database->addHook($hook);
        try {
            $database->createDocument($parents, new Document([
                Document::ID => self::NESTED_PREPARATION_MARKER,
                'name' => 'linked',
                'owner' => self::NESTED_PREPARATION_MARKER,
            ]));
        } finally {
            $database->removeHook($hook);
        }

        $linked = $database->getDocument($parents, self::NESTED_PREPARATION_MARKER);
        $this->assertSame('linked', $linked->getAttribute('name'), 'The top-level ID is stored as given');
        $owner = $linked->getAttribute('owner');
        $this->assertInstanceOf(Document::class, $owner);
        $this->assertSame(self::NESTED_PREPARATION_MARKER, $owner->getId());
        $this->assertSame('literal', $owner->getAttribute('name'));
        $this->assertCount(1, $database->find($owners));
        $this->assertSame([], $this->nestedPreparationSeen);
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

    public function testNestedHookRunsInThePreChecksBeforeTheGrantCheck(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, $children] = $this->createNestedPreparationHookCollections($database);
        $unheld = [Permission::read(Role::user('nestedPreparationUnheld'))];
        $database->getAuthorization()->skip(fn (): Document => $database->createDocument($children, new Document([
            Document::ID => self::NESTED_PREPARATION_MARKER,
            'name' => 'stored',
            Document::PERMISSIONS => $unheld,
        ])));
        $checked = static fn (callable $callback): mixed => $database->withCheckGrants(true, $callback);

        $this->assertNull($this->nestedPreparationRefusal(fn () => $checked(fn () => $database->authorizeCreate($parents, $this->nestedPreparationMarkedParent('unhooked', $unheld)))), 'Without the hook the stored document keeps its permissions');

        $hook = $this->nestedPreparationHook();
        $database->addHook($hook);
        try {
            $this->assertSame($this->nestedPreparationUnheldRefusal($database), $this->nestedPreparationRefusal(fn () => $checked(fn () => $database->authorizeCreate($parents, $this->nestedPreparationMarkedParent('checked', $unheld)))));
            $this->assertSame($this->nestedPreparationUnheldRefusal($database), $this->nestedPreparationRefusal(fn () => $checked(fn (): Document => $database->createDocument($parents, $this->nestedPreparationMarkedParent('refused', $unheld)))));
            $this->assertNull($this->nestedPreparationRefusal(fn () => $checked(fn () => $database->authorizeCreate($parents, $this->nestedPreparationMarkedParent('created', [Permission::read(Role::any())])))));
            $checked(fn (): Document => $database->createDocument($parents, $this->nestedPreparationMarkedParent('created', [Permission::read(Role::any())])));
        } finally {
            $database->removeHook($hook);
        }

        $this->assertTrue($database->getDocument($parents, 'refused')->isEmpty());
        $created = $this->nestedPreparationIds($database->getDocument($parents, 'created')->getAttribute('children'));
        $this->assertCount(1, $created);
        $this->assertNotSame(self::NESTED_PREPARATION_MARKER, $created[0]);
        $this->assertSame([Permission::read(Role::any())], $database->getDocument($children, $created[0])->getPermissions());
        $this->assertCount(2, $database->getAuthorization()->skip(fn (): array => $database->find($children)));
        $this->assertSame($unheld, $database->getAuthorization()->skip(fn (): Document => $database->getDocument($children, self::NESTED_PREPARATION_MARKER))->getPermissions());
        $this->assertSame(\array_fill(0, 4, [$children, self::NESTED_PREPARATION_MARKER]), $this->nestedPreparationSeen);
    }

    public function testNestedPreparationRefusesRelatedDocumentsInABulkUpdate(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, $children, $owners] = $this->createNestedPreparationCollections($database);
        $database->createDocument($parents, new Document([Document::ID => 'bulk', 'name' => 'bulk']));
        $permissions = [Permission::update(Role::user('nestedPreparationUnheld'))];
        $values = [
            'children' => [new Document([Document::ID => self::NESTED_PREPARATION_MARKER, 'name' => 'bulk child', Document::PERMISSIONS => $permissions])],
            'owner' => ['name' => 'bulk owner', Document::PERMISSIONS => $permissions],
        ];

        foreach ($values as $key => $value) {
            try {
                $database->withCheckGrants(true, fn (): int => $database->updateDocuments($parents, new Document([$key => $value])));
                $this->fail("A bulk update with {$key} given as a document is refused while grants are checked");
            } catch (RelationshipException $error) {
                $this->assertSame('Invalid relationship value. A bulk update takes related documents by ID while grants are checked.', $error->getMessage());
            }
        }

        $this->assertSame([], $database->find($children));
        $this->assertSame([], $database->find($owners));
        $stored = $database->getDocument($parents, 'bulk');
        $this->assertSame([], $this->nestedPreparationIds($stored->getAttribute('children')));
        $this->assertNull($stored->getAttribute('owner'));

        $database->createDocument($owners, new Document([Document::ID => 'linked owner', 'name' => 'linked owner']));
        $database->withCheckGrants(true, fn (): int => $database->updateDocuments(
            $parents,
            new Document(['owner' => 'linked owner']),
            [Query::equal(Document::ID, ['bulk'])],
        ));

        $this->assertSame(['linked owner'], $this->nestedPreparationIds($database->getDocument($parents, 'bulk')->getAttribute('owner')));
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
     * Parents have children (one to many, back by parent), an owner (many to one, back by parents) and a profile
     * (one to one, back by parent); children have tags (many to many, back by children).
     *
     * @return array{string, string, string, string, string} The parent, child, owner, profile and tag collection IDs
     */
    private function createNestedPreparationHookCollections(Database $database): array
    {
        $parents = Id::unique();
        $children = Id::unique();
        $owners = Id::unique();
        $profiles = Id::unique();
        $tags = Id::unique();
        $permissions = [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];

        foreach ([$parents, $children, $owners, $profiles, $tags] as $collection) {
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
            twoWay: true,
            twoWayKey: 'parents',
            onDelete: RelationshipDeleteAction::SetNull,
        ));
        $database->createRelationship($parents, Relationship::oneToOne(
            relatedCollection: $profiles,
            key: 'profile',
            twoWay: true,
            twoWayKey: 'parent',
            onDelete: RelationshipDeleteAction::SetNull,
        ));
        $database->createRelationship($children, Relationship::manyToMany(
            relatedCollection: $tags,
            key: 'tags',
            twoWay: true,
            twoWayKey: 'children',
            onDelete: RelationshipDeleteAction::SetNull,
        ));

        return [$parents, $children, $owners, $profiles, $tags];
    }

    /**
     * A hook that records each related document's collection and ID and replaces the marker ID with a generated one.
     */
    private function nestedPreparationHook(): Nested
    {
        return new class (self::NESTED_PREPARATION_MARKER, $this->recordNestedPreparation(...)) implements Nested {
            /**
             * @param  Closure(string, string): void  $record
             */
            public function __construct(
                private readonly string $marker,
                private readonly Closure $record,
            ) {
            }

            #[\Override]
            public function prepare(Document $collection, Document $document): Document
            {
                ($this->record)($collection->getId(), $document->getId());

                return $document->getId() === $this->marker ? $document->setAttribute(Document::ID, Id::unique()) : $document;
            }
        };
    }

    private function recordNestedPreparation(string $collection, string $id): void
    {
        $this->nestedPreparationSeen[] = [$collection, $id];
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function nestedPreparationMarked(string $name, array $attributes = []): Document
    {
        return new Document([Document::ID => self::NESTED_PREPARATION_MARKER, 'name' => $name, ...$attributes]);
    }

    /**
     * @param  list<string>  $permissions  The permissions of the parent's child, stored under the marker
     */
    private function nestedPreparationMarkedParent(string $id, array $permissions): Document
    {
        return new Document([
            Document::ID => $id,
            'name' => $id,
            'children' => [new Document([Document::ID => self::NESTED_PREPARATION_MARKER, 'name' => 'stored', Document::PERMISSIONS => $permissions])],
        ]);
    }

    /**
     * The only document named $name in $collection.
     */
    private function nestedPreparationNamed(Database $database, string $collection, string $name): Document
    {
        $matches = $database->getAuthorization()->skip(fn (): array => $database->find($collection, [Query::equal('name', [$name])]));

        $this->assertCount(1, $matches, "One document named {$name}");
        $this->assertNotSame(self::NESTED_PREPARATION_MARKER, $matches[0]->getId());
        $this->assertNotSame('', $matches[0]->getId());

        return $matches[0];
    }

    /**
     * @param  list<string>  $collections
     */
    private function assertNothingStoredUnderTheNestedPreparationMarker(Database $database, array $collections): void
    {
        foreach ($collections as $collection) {
            $this->assertTrue($database->getAuthorization()->skip(fn (): Document => $database->getDocument($collection, self::NESTED_PREPARATION_MARKER))->isEmpty(), "Nothing is stored under the marker in {$collection}");
        }
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
