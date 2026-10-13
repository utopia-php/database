<?php

namespace Tests\Unit\Relationships;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Hook\Nested;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Id;
use Utopia\Database\Permission;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipType;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

/**
 * Database::authorizeCreate() and authorizeUpdate() prepare the related documents they are given as the write
 * would: the registered Hook\Nested hooks run first, so a related document they give an ID that is not stored is
 * judged new, and with grants checked a related document may only add permissions for roles the caller holds, its
 * current permissions being those of the staged view when a resolver gives one.
 */
final class NestedPreCheckTest extends TestCase
{
    private const string USER = 'u1';

    private const string UNHELD_REFUSAL = 'Permissions must be one of: (any, users, user:u1)';

    private const string NO_CREATE = "No permissions provided for action 'create'";

    private const string RELATED = 'related';

    private const string STORED = 'stored';

    private const string LEAVES = 'leaves';

    private const string NEXT = 'next';

    private const string BACK = 'back';

    private const string MARKER = 'marker';

    private Authorization $authorization;

    private Database $database;

    /**
     * @var list<array{string, string}> The collection and ID of each related document the hook was given, in order
     */
    private array $seen = [];

    /**
     * @var list<array{string, string}> The collection and ID of each related document resolved, in order
     */
    private array $resolved = [];

    /**
     * @return iterable<string, array{Closure(): Adapter, RelationshipType}>
     */
    public static function types(): iterable
    {
        $adapters = [
            'memory' => static fn (): Adapter => new Memory(),
            'sqlite' => static fn (): Adapter => new SQLite(new PDO('sqlite::memory:')),
        ];

        foreach ($adapters as $adapterName => $adapter) {
            foreach (RelationshipType::cases() as $type) {
                yield "{$adapterName}, {$type->value}" => [$adapter, $type];
            }
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAnUnheldGrantIsRefusedAndNothingIsWritten(Closure $adapter, RelationshipType $type): void
    {
        $this->seed($adapter(), $type, [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())], null);
        $this->database->setCheckGrants(true);
        $related = new Document(['name' => 'new', Document::PERMISSIONS => [Permission::read(Role::user('u2'))]]);
        $old = $this->storedParent();

        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $this->database->authorizeCreate('parents', $this->parent($type, $related))));
        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $this->database->authorizeUpdate('parents', $old, new Document([self::RELATED => self::value($type, $related)]))));
        $this->assertSame([], $this->relatedNames());
        $this->assertNull($this->refusal(fn () => $this->database->withCheckGrants(false, fn () => $this->database->authorizeCreate('parents', $this->parent($type, $related)))), 'With the toggle off the grant is not checked');
        $this->assertNull($this->refusal(fn () => $this->authorization->skip(fn () => $this->database->authorizeCreate('parents', $this->parent($type, $related)))), 'With authorization disabled the grant is not checked');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAStagedViewsPermissionsCountAsCurrent(Closure $adapter, RelationshipType $type): void
    {
        $foreign = [Permission::read(Role::user('u2'))];
        $this->seed($adapter(), $type, [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())], []);
        $this->database->setCheckGrants(true);
        $staged = fn (string $collection, string $id): ?Document => $collection === self::RELATED && $id === self::STORED
            ? new Document([Document::ID => self::STORED, Document::COLLECTION => self::RELATED, 'name' => 'Stored', Document::PERMISSIONS => $foreign])
            : null;
        $parent = $this->parent($type, new Document([Document::ID => self::STORED, 'name' => 'Changed', Document::PERMISSIONS => $foreign]));

        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)), 'Judged as stored, the role is granted anew');
        $this->assertNull($this->refusal(fn () => $this->database->authorizeCreate('parents', $parent, $staged)), 'Judged as staged, the role is sent back unchanged');
        $this->assertNull($this->refusal(fn () => $this->database->authorizeUpdate('parents', $this->storedParent(), new Document([self::RELATED => $parent->getAttribute(self::RELATED)]), $staged)));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testADocumentStagedForDeletionIsNew(Closure $adapter, RelationshipType $type): void
    {
        $foreign = [Permission::read(Role::user('u2'))];
        $this->seed($adapter(), $type, [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())], $foreign);
        $this->database->setCheckGrants(true);
        $deleted = static fn (string $collection, string $id): ?Document => $collection === self::RELATED && $id === self::STORED ? new Document() : null;
        $parent = $this->parent($type, new Document([Document::ID => self::STORED, 'name' => 'Recreated', Document::PERMISSIONS => $foreign]));

        $this->assertNull($this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)), 'Judged as stored, the role is sent back unchanged');
        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $this->database->authorizeCreate('parents', $parent, $deleted)), 'Judged as staged, the document is new and every role is granted anew');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testANestedHookRewritesInThePreChecks(Closure $adapter, RelationshipType $type): void
    {
        $this->chain($adapter(), $type, [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())]);
        $old = $this->storedParent();
        $this->database->addHook($this->marker());
        $resolve = $this->resolveAsStored(...);
        $leaf = new Document([Document::ID => self::MARKER, 'name' => 'leaf']);
        $related = new Document([Document::ID => self::MARKER, 'name' => 'related', self::NEXT => self::value($type, $leaf)]);
        $parent = $this->parent($type, $related);

        $this->database->authorizeCreate('parents', $parent, $resolve);
        $this->database->authorizeUpdate('parents', $old, new Document([self::RELATED => self::value($type, $related)]), $resolve);
        $this->database->authorizeCreate(self::LEAVES, new Document([
            Document::ID => 'leaf',
            self::BACK => self::reverse($type, new Document([
                Document::ID => self::MARKER,
                'name' => 'inverse related',
                self::BACK => self::reverse($type, new Document([Document::ID => self::MARKER])),
            ])),
        ]), $resolve);

        $forward = [[self::RELATED, self::MARKER], [self::LEAVES, self::MARKER]];
        $this->assertSame([...$forward, ...$forward, [self::RELATED, self::MARKER], ['parents', self::MARKER]], $this->seen);
        $this->assertSame([self::RELATED, self::LEAVES, self::RELATED, self::LEAVES, self::RELATED, 'parents'], \array_column($this->resolved, 0), 'Each related document is resolved once, by its rewritten ID');
        $ids = \array_column($this->resolved, 1);
        $this->assertNotContains(self::MARKER, $ids);
        $this->assertNotContains('', $ids);
        $this->assertCount(\count($ids), \array_unique($ids));
        $this->assertSame([self::MARKER], self::ids($parent->getAttribute(self::RELATED)), 'The document checked is not changed');
        $this->assertSame(self::MARKER, $related->getId());
        $this->assertSame(self::MARKER, $leaf->getId());
        $this->assertSame([], $this->relatedNames());
        $this->assertSame([], $this->leafNames());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testARewrittenIdNeedsCreateWhereTheMarkedDocumentNeedsUpdate(Closure $adapter, RelationshipType $type): void
    {
        $this->chain($adapter(), $type, [Permission::read(Role::any()), Permission::update(Role::any())]);
        $this->authorization->skip(fn () => $this->database->createDocument(self::RELATED, new Document([Document::ID => self::MARKER, 'name' => 'Literal'])));
        $parent = $this->parent($type, new Document([Document::ID => self::MARKER, 'name' => 'Changed']));

        $this->assertNull($this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)), 'The marked document is stored, so it is linked and changed, which needs update');
        $this->database->addHook($this->marker());
        $this->assertSame(self::NO_CREATE, $this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)), 'A rewritten ID is new, which needs create');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testTheGrantCheckRunsAfterTheRewrite(Closure $adapter, RelationshipType $type): void
    {
        $foreign = [Permission::read(Role::user('u2'))];
        $this->chain($adapter(), $type, [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())]);
        $this->authorization->skip(fn () => $this->database->createDocument(self::RELATED, new Document([Document::ID => self::MARKER, 'name' => 'Stored', Document::PERMISSIONS => $foreign])));
        $old = $this->storedParent();
        $this->database->setCheckGrants(true);
        $related = new Document([Document::ID => self::MARKER, 'name' => 'Stored', Document::PERMISSIONS => $foreign]);
        $parent = $this->parent($type, $related);
        $changes = new Document([self::RELATED => self::value($type, $related)]);
        $staged = static fn (string $collection, string $id): ?Document => $collection === self::RELATED && $id === self::MARKER
            ? new Document([Document::ID => self::MARKER, Document::COLLECTION => self::RELATED, 'name' => 'Stored', Document::PERMISSIONS => $foreign])
            : null;

        $this->assertNull($this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)), 'Judged as the marked document, the role is sent back unchanged');
        $this->assertNull($this->refusal(fn () => $this->database->authorizeUpdate('parents', $old, $changes)));

        $this->database->addHook($this->marker());

        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)), 'Judged after the rewrite, the document is new and every role is granted anew');
        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $this->database->authorizeUpdate('parents', $old, $changes)));
        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $this->database->authorizeCreate('parents', $parent, $staged)), 'A staged view of the marker is not the rewritten document');
        $this->assertSame([], $this->relatedNames());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAnIdTheHookAssignsIsJudgedNewWithoutTheStoredOrStagedPermissions(Closure $adapter, RelationshipType $type): void
    {
        $foreign = [Permission::read(Role::user('u2'))];
        $this->seed($adapter(), $type, [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())], $foreign);
        $this->database->setCheckGrants(true)->addHook($this->assigning(self::STORED));
        $staged = static fn (string $collection, string $id): ?Document => $collection === self::RELATED && $id === self::STORED
            ? new Document([Document::ID => self::STORED, Document::COLLECTION => self::RELATED, 'name' => 'Stored', Document::PERMISSIONS => $foreign])
            : null;
        $kept = $this->parent($type, new Document([Document::ID => self::STORED, 'name' => 'Stored', Document::PERMISSIONS => $foreign]));
        $assigned = $this->parent($type, new Document([Document::ID => self::MARKER, 'name' => 'Stored', Document::PERMISSIONS => $foreign]));

        $this->assertNull($this->refusal(fn () => $this->database->authorizeCreate('parents', $kept)), 'An ID the hook keeps is judged by its stored permissions');
        $this->assertNull($this->refusal(fn () => $this->database->authorizeCreate('parents', $kept, $staged)), 'An ID the hook keeps is judged by its staged permissions');
        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $this->database->authorizeCreate('parents', $assigned)), 'The stored permissions of an ID the hook assigns are not read');
        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $this->database->authorizeCreate('parents', $assigned, $staged)), 'The staged permissions of an ID the hook assigns are not resolved');
        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $this->database->authorizeUpdate('parents', $this->storedParent(), new Document([self::RELATED => $assigned->getAttribute(self::RELATED)]), $staged)));
        $this->assertSame([], $this->relatedNames());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAnIdTheHookAssignsNeedsCreateWhileGrantsAreChecked(Closure $adapter, RelationshipType $type): void
    {
        $held = [Permission::read(Role::user(self::USER))];
        $related = new Document([Document::ID => self::MARKER, 'name' => 'Created', Document::PERMISSIONS => $held]);

        $this->chain($adapter(), $type, [Permission::read(Role::any()), Permission::update(Role::any())]);
        $this->database->setCheckGrants(true)->addHook($this->marker());
        $this->assertSame(self::NO_CREATE, $this->refusal(fn () => $this->database->authorizeCreate('parents', $this->parent($type, $related))));
        $this->assertSame(self::NO_CREATE, $this->refusal(fn () => $this->database->createDocument('parents', $this->parent($type, $related))));
        $this->assertSame([], $this->relatedNames());

        $this->chain($adapter(), $type, [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())]);
        $this->database->setCheckGrants(true)->addHook($this->marker());
        $this->assertNull($this->refusal(fn () => $this->database->authorizeCreate('parents', $this->parent($type, $related))));
        $created = self::ids($this->database->createDocument('parents', $this->parent($type, $related))->getAttribute(self::RELATED));
        $this->assertCount(1, $created);
        $this->assertNotSame(self::MARKER, $created[0]);
        $this->assertSame($held, $this->authorization->skip(fn (): Document => $this->database->getDocument(self::RELATED, $created[0]))->getPermissions());
        $this->assertSame(['Created'], $this->relatedNames());
    }

    /**
     * @param  list<string>  $collectionPermissions
     * @param  list<string>|null  $storedPermissions  The related document's permissions; null stores none
     */
    private function seed(Adapter $adapter, RelationshipType $type, array $collectionPermissions, ?array $storedPermissions): void
    {
        $this->connect($adapter);

        $this->authorization->skip(function () use ($type, $collectionPermissions, $storedPermissions): void {
            $this->createCollections([self::RELATED], $collectionPermissions);
            $this->database->createRelationship('parents', match ($type) {
                RelationshipType::OneToOne => Relationship::oneToOne(self::RELATED, self::RELATED, onDelete: RelationshipDeleteAction::SetNull),
                RelationshipType::ManyToOne => Relationship::manyToOne(self::RELATED, self::RELATED, onDelete: RelationshipDeleteAction::SetNull),
                RelationshipType::OneToMany => Relationship::oneToMany(self::RELATED, self::RELATED, twoWay: true, twoWayKey: self::BACK, onDelete: RelationshipDeleteAction::SetNull),
                RelationshipType::ManyToMany => Relationship::manyToMany(self::RELATED, self::RELATED, twoWay: true, twoWayKey: self::BACK, onDelete: RelationshipDeleteAction::SetNull),
            });
            if ($storedPermissions !== null) {
                $this->database->createDocument(self::RELATED, new Document([Document::ID => self::STORED, 'name' => 'Stored', Document::PERMISSIONS => $storedPermissions]));
            }
        });
    }

    /**
     * Parents relate RELATED, which relate LEAVES, the same way, each related back by BACK.
     *
     * @param  list<string>  $relatedPermissions  The permissions of the RELATED and LEAVES collections
     */
    private function chain(Adapter $adapter, RelationshipType $type, array $relatedPermissions): void
    {
        $this->connect($adapter);

        $this->authorization->skip(function () use ($type, $relatedPermissions): void {
            $this->createCollections([self::RELATED, self::LEAVES], $relatedPermissions);
            $this->database->createRelationship('parents', self::twoWayRelationship($type, self::RELATED, self::RELATED));
            $this->database->createRelationship(self::RELATED, self::twoWayRelationship($type, self::LEAVES, self::NEXT));
        });
    }

    private function connect(Adapter $adapter): void
    {
        $this->authorization = new Authorization();
        $this->authorization->addRole(Role::users()->toString());
        $this->authorization->addRole(Role::user(self::USER)->toString());
        $this->database = (new Database($adapter, new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('nested_pre_check')
            ->setNamespace('nested_pre_check_'.\uniqid());
        $this->database->addHook(new Permissions());
        $this->database->addHook(new Relationships());
    }

    /**
     * Creates the parents collection and the $related collections, which have $permissions.
     *
     * @param  list<string>  $related
     * @param  list<string>  $permissions
     */
    private function createCollections(array $related, array $permissions): void
    {
        $this->database->create();
        $this->database->createCollection(Collection::create(
            'parents',
            attributes: [Attribute::string('name', size: 64)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));
        foreach ($related as $collection) {
            $this->database->createCollection(Collection::create(
                $collection,
                attributes: [Attribute::string('name', size: 64)],
                permissions: $permissions,
                documentSecurity: false,
            ));
        }
    }

    private static function twoWayRelationship(RelationshipType $type, string $related, string $key): Relationship
    {
        return match ($type) {
            RelationshipType::OneToOne => Relationship::oneToOne($related, $key, twoWay: true, twoWayKey: self::BACK, onDelete: RelationshipDeleteAction::SetNull),
            RelationshipType::ManyToOne => Relationship::manyToOne($related, $key, twoWay: true, twoWayKey: self::BACK, onDelete: RelationshipDeleteAction::SetNull),
            RelationshipType::OneToMany => Relationship::oneToMany($related, $key, twoWay: true, twoWayKey: self::BACK, onDelete: RelationshipDeleteAction::SetNull),
            RelationshipType::ManyToMany => Relationship::manyToMany($related, $key, twoWay: true, twoWayKey: self::BACK, onDelete: RelationshipDeleteAction::SetNull),
        };
    }

    /**
     * A hook that records each related document it is given and replaces a MARKER ID with a generated one.
     */
    private function marker(): Nested
    {
        return new class (self::MARKER, $this->record(...)) implements Nested {
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

    /**
     * A hook that replaces a MARKER ID with $id.
     */
    private function assigning(string $id): Nested
    {
        return new class (self::MARKER, $id) implements Nested {
            public function __construct(
                private readonly string $marker,
                private readonly string $id,
            ) {
            }

            #[\Override]
            public function prepare(Document $collection, Document $document): Document
            {
                return $document->getId() === $this->marker ? $document->setAttribute(Document::ID, $this->id) : $document;
            }
        };
    }

    private function record(string $collection, string $id): void
    {
        $this->seen[] = [$collection, $id];
    }

    private function resolveAsStored(string $collection, string $id): null
    {
        $this->resolved[] = [$collection, $id];

        return null;
    }

    private function parent(RelationshipType $type, Document $related): Document
    {
        return new Document([
            Document::ID => 'parent',
            Document::PERMISSIONS => [Permission::read(Role::any())],
            self::RELATED => self::value($type, $related),
        ]);
    }

    private function storedParent(): Document
    {
        $stored = $this->authorization->skip(fn (): Document => $this->database->getDocument('parents', 'parent'));
        if (! $stored->isEmpty()) {
            return $stored;
        }

        return $this->authorization->skip(fn (): Document => $this->database->createDocument('parents', new Document([
            Document::ID => 'parent',
            Document::PERMISSIONS => [Permission::read(Role::any()), Permission::update(Role::any())],
        ])));
    }

    /**
     * @return Document|list<Document>
     */
    private static function value(RelationshipType $type, Document $related): Document|array
    {
        return \in_array($type, [RelationshipType::OneToMany, RelationshipType::ManyToMany], true) ? [$related] : $related;
    }

    /**
     * The value of BACK, the inverse side of a relationship of $type, holding $related.
     *
     * @return Document|list<Document>
     */
    private static function reverse(RelationshipType $type, Document $related): Document|array
    {
        return \in_array($type, [RelationshipType::ManyToOne, RelationshipType::ManyToMany], true) ? [$related] : $related;
    }

    /**
     * @return list<string>
     */
    private function leafNames(): array
    {
        return $this->authorization->skip(fn (): array => \array_values(\array_map(
            static fn (Document $document): string => \is_string($name = $document->getAttribute('name')) ? $name : '',
            $this->database->find(self::LEAVES),
        )));
    }

    /**
     * @return list<string>
     */
    private function relatedNames(): array
    {
        $names = $this->authorization->skip(fn (): array => \array_map(
            static fn (Document $document): string => \is_string($name = $document->getAttribute('name')) ? $name : '',
            $this->database->find(self::RELATED),
        ));

        return \array_values(\array_filter($names, static fn (string $name): bool => $name !== 'Stored'));
    }

    /**
     * @return list<string>
     */
    private static function ids(mixed $value): array
    {
        $values = \is_array($value) ? $value : [$value];

        return \array_values(\array_map(static fn (mixed $item): string => $item instanceof Document ? $item->getId() : (\is_string($item) ? $item : ''), $values));
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
