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
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Permission;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

/**
 * Database::authorizeCreate() and authorizeUpdate() given a resolver judge every related document as the writes
 * staged before them leave it, and decide what the write decides once those staged writes have run: a related
 * document they delete is new (create on its collection), one they change or create is judged by its staged
 * permissions, and one they leave alone is judged as stored.
 */
final class StagedViewAuthorizationTest extends TestCase
{
    private const string USER = 'u1';

    private const string NO_CREATE = "No permissions provided for action 'create'";

    private const string NO_READ = "No permissions provided for action 'read'";

    private const string NO_UPDATE = "No permissions provided for action 'update'";

    private const string ONLY_ADMINS_UPDATE = 'Missing "update" permission for role "team:admin". Only "["any","users","user:u1"]" scopes are allowed and "["team:admin"]" was given.';

    private const string UNSTAGED = 'unstaged';

    private const string DELETED = 'deleted';

    private const string UPDATED = 'updated';

    private const string CREATED = 'created';

    private const string ID = 'id';

    private const string DOCUMENT = 'document';

    private const array TYPES = ['oneToOne', 'oneToOne one-way', 'oneToMany', 'manyToOne', 'manyToMany'];

    private Authorization $authorization;

    private Database $database;

    /**
     * @return iterable<string, array{Closure(): Adapter, string, string, bool, list<string>, list<string>|null, list<string>|null, string, ?string}>
     */
    public static function cases(): iterable
    {
        $users = Role::users();
        $user = Role::user(self::USER);
        $createRead = [Permission::create($users), Permission::read($users)];

        $scenarios = [
            'unstaged, update on the collection' => [self::UNSTAGED, false, [...$createRead, Permission::update($users)], [], null, null, null],
            'unstaged, read without update' => [self::UNSTAGED, true, $createRead, [], null, self::NO_UPDATE, self::NO_UPDATE],
            'staged deleted, create on the collection, update only for another team' => [self::DELETED, true, [Permission::create(Role::any()), Permission::read(Role::any()), Permission::delete(Role::any())], [Permission::update(Role::team('admin'))], null, null, null],
            'staged deleted, no create on the collection' => [self::DELETED, false, [Permission::read($users), Permission::update($users)], [], null, null, self::NO_CREATE],
            'staged update revokes update' => [self::UPDATED, true, $createRead, [Permission::update($user)], [], self::NO_UPDATE, self::NO_UPDATE],
            'staged update grants update' => [self::UPDATED, true, $createRead, [], [Permission::update($user)], null, null],
            'staged update hides it' => [self::UPDATED, true, [Permission::create($users)], [Permission::read($user), Permission::update($user)], [Permission::update($user)], self::NO_READ, self::NO_READ],
            'staged update reveals it' => [self::UPDATED, true, [Permission::create($users)], [], [Permission::read($user), Permission::update($user)], null, null],
            'staged create without update' => [self::CREATED, true, $createRead, null, [], self::NO_UPDATE, self::NO_UPDATE],
            'staged create with update' => [self::CREATED, true, $createRead, null, [Permission::update($user)], null, null],
        ];

        $adapters = [
            'memory' => static fn (): Adapter => new Memory(),
            'sqlite' => static fn (): Adapter => new SQLite(new PDO('sqlite::memory:')),
        ];

        foreach ($adapters as $adapterName => $adapter) {
            foreach (self::TYPES as $type) {
                foreach ($scenarios as $scenario => [$state, $documentSecurity, $collectionPermissions, $storedPermissions, $stagedPermissions, $byId, $asDocument]) {
                    yield "{$adapterName}, {$type} by ID, {$scenario}" => [$adapter, $type, $state, $documentSecurity, $collectionPermissions, $storedPermissions, $stagedPermissions, self::ID, $byId];
                    yield "{$adapterName}, {$type} as a document, {$scenario}" => [$adapter, $type, $state, $documentSecurity, $collectionPermissions, $storedPermissions, $stagedPermissions, self::DOCUMENT, $asDocument];
                }
            }
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  list<string>  $collectionPermissions
     * @param  list<string>|null  $storedPermissions
     * @param  list<string>|null  $stagedPermissions
     */
    #[DataProvider('cases')]
    public function testAuthorizeCreateJudgesTheStagedView(Closure $adapter, string $type, string $state, bool $documentSecurity, array $collectionPermissions, ?array $storedPermissions, ?array $stagedPermissions, string $value, ?string $refusal): void
    {
        $this->seed($adapter(), $type, $documentSecurity, $collectionPermissions, $storedPermissions);
        $resolve = $this->resolver($state, $stagedPermissions);

        $this->assertSame($refusal, $this->refusal(fn () => $this->asUser(fn () => $this->database->authorizeCreate('parents', $this->parent($type, $value), $resolve))));
        $this->assertWroteNothing($storedPermissions !== null);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  list<string>  $collectionPermissions
     * @param  list<string>|null  $storedPermissions
     * @param  list<string>|null  $stagedPermissions
     */
    #[DataProvider('cases')]
    public function testTheCreateDecidesTheSameAfterTheStagedWrites(Closure $adapter, string $type, string $state, bool $documentSecurity, array $collectionPermissions, ?array $storedPermissions, ?array $stagedPermissions, string $value, ?string $refusal): void
    {
        $this->seed($adapter(), $type, $documentSecurity, $collectionPermissions, $storedPermissions);

        $this->assertSame($refusal, $this->refusal(fn () => $this->database->withTransaction(function () use ($state, $stagedPermissions, $type, $value): void {
            $this->applyStaged($state, $stagedPermissions);
            $this->asUser(fn () => $this->database->createDocument('parents', $this->parent($type, $value)));
        })));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  list<string>  $collectionPermissions
     * @param  list<string>|null  $storedPermissions
     * @param  list<string>|null  $stagedPermissions
     */
    #[DataProvider('cases')]
    public function testAuthorizeUpdateJudgesTheStagedView(Closure $adapter, string $type, string $state, bool $documentSecurity, array $collectionPermissions, ?array $storedPermissions, ?array $stagedPermissions, string $value, ?string $refusal): void
    {
        $this->seed($adapter(), $type, $documentSecurity, $collectionPermissions, $storedPermissions);
        $old = $this->storedParent();
        $resolve = $this->resolver($state, $stagedPermissions);

        $this->assertSame($refusal, $this->refusal(fn () => $this->asUser(fn () => $this->database->authorizeUpdate('parents', $old, $this->update($type, $value), $resolve))));
        $this->assertSame([], self::ids($this->authorization->skip(fn (): Document => $this->database->getDocument('parents', 'parent'))->getAttribute('related')), 'The check links nothing');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  list<string>  $collectionPermissions
     * @param  list<string>|null  $storedPermissions
     * @param  list<string>|null  $stagedPermissions
     */
    #[DataProvider('cases')]
    public function testTheUpdateDecidesTheSameAfterTheStagedWrites(Closure $adapter, string $type, string $state, bool $documentSecurity, array $collectionPermissions, ?array $storedPermissions, ?array $stagedPermissions, string $value, ?string $refusal): void
    {
        $this->seed($adapter(), $type, $documentSecurity, $collectionPermissions, $storedPermissions);
        $this->storedParent();

        $this->assertSame($refusal, $this->refusal(fn () => $this->database->withTransaction(function () use ($state, $stagedPermissions, $type, $value): void {
            $this->applyStaged($state, $stagedPermissions);
            $this->asUser(fn () => $this->database->updateDocument('parents', 'parent', $this->update($type, $value)));
        })));
    }

    public function testARelatedDocumentStagedForDeletionIsRecreatedWithTheGivenPermissions(): void
    {
        $foreign = [Permission::update(Role::team('admin'))];
        $this->seed(new Memory(), 'oneToOne one-way', true, [Permission::create(Role::any()), Permission::read(Role::any()), Permission::delete(Role::any())], $foreign);
        $parent = new Document(['$id' => 'parent', 'related' => ['$id' => 'stored', 'name' => 'Recreated', '$permissions' => $foreign]]);

        $this->assertSame(self::ONLY_ADMINS_UPDATE, $this->refusal(fn () => $this->asUser(fn () => $this->database->authorizeCreate('parents', $parent))), 'Judged as stored, the nested document is a link needing update');
        $this->assertNull($this->refusal(fn () => $this->asUser(fn () => $this->database->authorizeCreate('parents', $parent, $this->resolver(self::DELETED, null)))), 'Judged as staged, the nested document is new and needs create');

        $this->database->withTransaction(function () use ($parent): void {
            $this->applyStaged(self::DELETED, null);
            $this->asUser(fn () => $this->database->createDocument('parents', $parent));
        });

        $recreated = $this->authorization->skip(fn (): Document => $this->database->getDocument('related', 'stored'));
        $this->assertSame('Recreated', $recreated->getAttribute('name'));
        $this->assertSame($foreign, $recreated->getPermissions());
    }

    public function testAResolverReturningNullJudgesTheStoredDocument(): void
    {
        $this->seed(new Memory(), 'manyToOne', true, [Permission::create(Role::users()), Permission::read(Role::users())], [Permission::update(Role::user(self::USER))]);

        foreach ([self::ID, self::DOCUMENT] as $value) {
            $this->assertNull($this->refusal(fn () => $this->asUser(fn () => $this->database->authorizeCreate('parents', $this->parent('manyToOne', $value), static fn (string $collection, string $id): ?Document => null))));
        }
    }

    public function testAnApiKeyPassesWhateverTheStagedView(): void
    {
        $this->seed(new Memory(), 'oneToMany', true, [], []);

        foreach ([self::DELETED, self::UPDATED] as $state) {
            $resolve = $this->resolver($state, []);
            foreach ([self::ID, self::DOCUMENT] as $value) {
                $this->assertNull($this->refusal(fn () => $this->authorization->skip(fn () => $this->database->authorizeCreate('parents', $this->parent('oneToMany', $value), $resolve))));
            }
        }
    }

    /**
     * A document a staged write already links to the related document is not linked again, so only changing it
     * needs update, as for the stored document an update changes.
     */
    public function testANestedDocumentIsJudgedAgainstTheLinksTheStagedWritesLeave(): void
    {
        $this->seed(new Memory(), 'oneToMany', false, [Permission::create(Role::users()), Permission::read(Role::users()), Permission::update(Role::users())], []);
        $this->authorization->skip(function (): void {
            $this->database->createCollection(Collection::create('toys', attributes: [Attribute::string('name', size: 64)], permissions: [Permission::read(Role::users())]));
            $this->database->createRelationship('related', Relationship::manyToOne('toys', 'toy', onDelete: RelationshipDeleteAction::SetNull));
            $this->database->createDocument('toys', new Document(['$id' => 'toy', 'name' => 'Toy']));
        });
        $parent = new Document(['$id' => 'parent', 'related' => [['$id' => 'stored', 'toy' => 'toy']]]);
        $linked = fn (string $collection, string $id): ?Document => $collection === 'related' && $id === 'stored'
            ? $this->authorization->skip(fn (): Document => $this->database->getDocument('related', 'stored'))->setAttribute('toy', 'toy')
            : null;

        $this->assertSame(self::NO_UPDATE, $this->refusal(fn () => $this->asUser(fn () => $this->database->authorizeCreate('parents', $parent))), 'Judged as stored, the toy is linked anew');
        $this->assertNull($this->refusal(fn () => $this->asUser(fn () => $this->database->authorizeCreate('parents', $parent, $linked))), 'Judged as staged, the toy is already linked');

        $this->database->withTransaction(function () use ($parent): void {
            $this->authorization->skip(fn () => $this->database->updateDocument('related', 'stored', new Document(['toy' => 'toy'])));
            $this->asUser(fn () => $this->database->createDocument('parents', $parent));
        });
        $this->assertSame(['stored'], self::ids($this->authorization->skip(fn (): Document => $this->database->getDocument('parents', 'parent'))->getAttribute('related')));
    }

    /**
     * The view the staged writes of $state leave of the related document 'stored'.
     *
     * @param  list<string>|null  $stagedPermissions
     * @return Closure(string, string): ?Document
     */
    private function resolver(string $state, ?array $stagedPermissions): Closure
    {
        return fn (string $collection, string $id): ?Document => $collection !== 'related' || $id !== 'stored' ? null : match ($state) {
            self::DELETED => new Document(),
            self::UPDATED, self::CREATED => new Document(['$id' => 'stored', '$collection' => 'related', 'name' => 'Stored', '$permissions' => $stagedPermissions ?? []]),
            default => null,
        };
    }

    /**
     * @param  list<string>|null  $stagedPermissions
     */
    private function applyStaged(string $state, ?array $stagedPermissions): void
    {
        $this->authorization->skip(fn (): mixed => match ($state) {
            self::DELETED => $this->database->deleteDocument('related', 'stored'),
            self::UPDATED => $this->database->updateDocument('related', 'stored', new Document(['$permissions' => $stagedPermissions ?? []])),
            self::CREATED => $this->database->createDocument('related', new Document(['$id' => 'stored', 'name' => 'Stored', '$permissions' => $stagedPermissions ?? []])),
            default => null,
        });
    }

    private function parent(string $type, string $value): Document
    {
        return new Document([
            '$id' => 'parent',
            '$permissions' => [Permission::read(Role::any())],
            'related' => $this->related($type, $value),
        ]);
    }

    private function update(string $type, string $value): Document
    {
        return new Document(['related' => $this->related($type, $value)]);
    }

    private function related(string $type, string $value): mixed
    {
        $related = $value === self::ID ? 'stored' : new Document(['$id' => 'stored', 'name' => 'Changed']);

        return \in_array($type, ['oneToMany', 'manyToMany'], true) ? [$related] : $related;
    }

    private function storedParent(): Document
    {
        return $this->authorization->skip(fn (): Document => $this->database->createDocument('parents', new Document([
            '$id' => 'parent',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
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

    private function assertWroteNothing(bool $stored): void
    {
        $this->assertTrue($this->authorization->skip(fn (): bool => $this->database->getDocument('parents', 'parent')->isEmpty()), 'The check writes no parent');
        $related = $this->authorization->skip(fn (): Document => $this->database->getDocument('related', 'stored'));
        if (! $stored) {
            $this->assertTrue($related->isEmpty(), 'The check creates no related document');

            return;
        }
        $this->assertSame('Stored', $related->getAttribute('name'), 'The check changes no related document');
        $this->assertSame([], self::ids($related->getAttribute('back')), 'The check links nothing');
    }

    /**
     * @param  list<string>  $collectionPermissions
     * @param  list<string>|null  $storedPermissions  The related document's permissions; null stores none
     */
    private function seed(Adapter $adapter, string $type, bool $documentSecurity, array $collectionPermissions, ?array $storedPermissions): void
    {
        $this->authorization = new Authorization();
        $this->database = (new Database($adapter, new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('staged_view')
            ->setNamespace('staged_view_'.\uniqid());
        $this->database->addHook(new Permissions());
        $this->database->addHook(new Relationships());

        $this->authorization->skip(function () use ($type, $documentSecurity, $collectionPermissions, $storedPermissions): void {
            $this->database->create();
            $this->database->createCollection(Collection::create(
                'parents',
                attributes: [Attribute::string('name', size: 64)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            ));
            $this->database->createCollection(Collection::create(
                'related',
                attributes: [Attribute::string('name', size: 64)],
                permissions: $collectionPermissions,
                documentSecurity: $documentSecurity,
            ));
            $this->database->createRelationship('parents', match ($type) {
                'oneToOne' => Relationship::oneToOne('related', 'related', twoWay: true, twoWayKey: 'back', onDelete: RelationshipDeleteAction::SetNull),
                'oneToOne one-way' => Relationship::oneToOne('related', 'related', onDelete: RelationshipDeleteAction::SetNull),
                'oneToMany' => Relationship::oneToMany('related', 'related', twoWay: true, twoWayKey: 'back', onDelete: RelationshipDeleteAction::SetNull),
                'manyToOne' => Relationship::manyToOne('related', 'related', twoWay: true, twoWayKey: 'back', onDelete: RelationshipDeleteAction::SetNull),
                default => Relationship::manyToMany('related', 'related', twoWay: true, twoWayKey: 'back', onDelete: RelationshipDeleteAction::SetNull),
            });
            if ($storedPermissions !== null) {
                $this->database->createDocument('related', new Document(['$id' => 'stored', 'name' => 'Stored', '$permissions' => $storedPermissions]));
            }
        });
    }

    private function asUser(callable $callback): mixed
    {
        return $this->authorization->withRoles([Role::any()->toString(), Role::users()->toString(), Role::user(self::USER)->toString()], $callback);
    }

    /**
     * @return list<string>
     */
    private static function ids(mixed $value): array
    {
        $values = \is_array($value) ? $value : ($value === null ? [] : [$value]);

        return \array_values(\array_map(static fn (mixed $item): string => $item instanceof Document ? $item->getId() : (\is_string($item) ? $item : ''), $values));
    }
}
