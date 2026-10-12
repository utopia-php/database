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
 * A nested write links or changes a stored related document only with update on it (collection-level, or
 * document-level under document security), whichever relationship type holds the link and whether the document
 * is sent by ID or as a document. A stored document the caller cannot read is refused, never linked, changed or
 * taken for a new one.
 */
final class NestedLinkPermissionsTest extends TestCase
{
    private const string USER = 'u1';

    private const string NO_READ = "No permissions provided for action 'read'";

    private const string NO_UPDATE = "No permissions provided for action 'update'";

    private const string GUEST_NO_READ = 'Missing "read" permission for role "users". Only "["any","guests"]" scopes are allowed and "["users"]" was given.';

    private const string GUEST_NO_UPDATE = 'Missing "update" permission for role "users". Only "["any","guests"]" scopes are allowed and "["users"]" was given.';

    private const string KEY = 'key';

    private const string SESSION = 'session';

    private const string GUEST = 'guest';

    private const string ID = 'id';

    private const string DOCUMENT = 'document';

    private Authorization $authorization;

    private Database $database;

    /**
     * @return iterable<string, array{Closure(): Adapter, string, bool, list<string>, list<string>, string, string, ?string, bool}>
     */
    public static function cases(): iterable
    {
        $read = [Permission::read(Role::users())];
        $update = [...$read, Permission::update(Role::users())];
        $own = [Permission::read(Role::user(self::USER)), Permission::update(Role::user(self::USER))];

        $scenarios = [
            'update on the collection' => [false, $update, [], self::SESSION, null, null],
            'read without update' => [false, $read, [], self::SESSION, self::NO_UPDATE, self::NO_UPDATE],
            'update on the document under document security' => [true, [], $own, self::SESSION, null, null],
            'read without update under document security' => [true, [], [Permission::read(Role::user(self::USER))], self::SESSION, self::NO_UPDATE, self::NO_UPDATE],
            'a document hidden by document security' => [true, [Permission::create(Role::users())], [], self::SESSION, self::NO_READ, self::NO_UPDATE],
            'update and create without read' => [false, [Permission::create(Role::users()), Permission::update(Role::users())], [], self::SESSION, self::NO_READ, self::NO_READ],
            'a guest where users may read and update' => [false, [...$update, Permission::create(Role::any())], [], self::GUEST, self::GUEST_NO_READ, self::GUEST_NO_UPDATE],
            'an API key without any permission' => [false, [], [], self::KEY, null, null],
        ];

        $adapters = [
            'memory' => static fn (): Adapter => new Memory(),
            'sqlite' => static fn (): Adapter => new SQLite(new PDO('sqlite::memory:')),
        ];

        $writes = [
            'memory' => ['batch create in a transaction' => true, 'single create' => false],
            'sqlite' => ['batch create in a transaction' => true],
        ];

        foreach ($adapters as $adapterName => $adapter) {
            foreach ($writes[$adapterName] as $writeName => $transaction) {
                foreach (['oneToOne', 'oneToOne one-way', 'oneToMany', 'manyToOne', 'manyToMany'] as $type) {
                    foreach ($scenarios as $scenario => [$documentSecurity, $collectionPermissions, $documentPermissions, $caller, $byId, $asDocument]) {
                        yield "{$adapterName}, {$writeName}, {$type} by ID, {$scenario}" => [$adapter, $type, $documentSecurity, $collectionPermissions, $documentPermissions, $caller, self::ID, $byId, $transaction];
                        yield "{$adapterName}, {$writeName}, {$type} as a document, {$scenario}" => [$adapter, $type, $documentSecurity, $collectionPermissions, $documentPermissions, $caller, self::DOCUMENT, $asDocument, $transaction];
                    }
                }
            }
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  list<string>  $collectionPermissions
     * @param  list<string>  $documentPermissions
     */
    #[DataProvider('cases')]
    public function testCreatingAParentWithAStoredRelatedDocument(Closure $adapter, string $type, bool $documentSecurity, array $collectionPermissions, array $documentPermissions, string $caller, string $value, ?string $refusal, bool $transaction): void
    {
        $this->seed($adapter(), $type, $documentSecurity, $collectionPermissions, $documentPermissions);

        $related = $value === self::ID ? 'stored' : new Document(['$id' => 'stored', 'name' => 'Changed']);
        $parent = new Document([
            '$id' => 'parent',
            '$permissions' => [Permission::read(Role::any())],
            'related' => \in_array($type, ['oneToMany', 'manyToMany'], true) ? [$related] : $related,
        ]);

        try {
            $this->as($caller, fn (): mixed => $transaction
                ? $this->database->withTransaction(fn (): int => $this->database->createDocuments('parents', [$parent]))
                : $this->database->createDocument('parents', $parent));
        } catch (AuthorizationException $error) {
            $this->assertSame($refusal, $error->getMessage(), 'The create is refused for the wrong reason');
            $this->assertTrue($this->authorization->skip(fn (): bool => $this->database->getDocument('parents', 'parent')->isEmpty()), 'A refused create writes no parent');
            $stored = $this->authorization->skip(fn (): Document => $this->database->getDocument('related', 'stored'));
            $this->assertSame('Stored', $stored->getAttribute('name'), 'A refused create leaves the stored document unchanged');
            $this->assertSame([], self::ids($stored->getAttribute('back')), 'A refused create leaves no link on the stored document');

            return;
        }

        $this->assertNull($refusal, 'The create must be refused with: '.$refusal);
        $this->assertSame(['stored'], self::ids($this->authorization->skip(fn (): Document => $this->database->getDocument('parents', 'parent'))->getAttribute('related')));
        if ($type !== 'oneToOne one-way') {
            $this->assertSame(['parent'], self::ids($this->authorization->skip(fn (): Document => $this->database->getDocument('related', 'stored'))->getAttribute('back')));
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  list<string>  $collectionPermissions
     * @param  list<string>  $documentPermissions
     */
    #[DataProvider('cases')]
    public function testUpdatingAParentToAStoredRelatedDocument(Closure $adapter, string $type, bool $documentSecurity, array $collectionPermissions, array $documentPermissions, string $caller, string $value, ?string $refusal, bool $transaction): void
    {
        $this->seed($adapter(), $type, $documentSecurity, $collectionPermissions, $documentPermissions);
        $this->authorization->skip(fn (): Document => $this->database->createDocument('parents', new Document([
            '$id' => 'parent',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
        ])));

        $related = $value === self::ID ? 'stored' : new Document(['$id' => 'stored', 'name' => 'Changed']);
        $update = new Document(['related' => \in_array($type, ['oneToMany', 'manyToMany'], true) ? [$related] : $related]);

        try {
            $this->as($caller, fn (): mixed => $transaction
                ? $this->database->withTransaction(fn (): Document => $this->database->updateDocument('parents', 'parent', $update))
                : $this->database->updateDocument('parents', 'parent', $update));
        } catch (AuthorizationException $error) {
            $this->assertSame($refusal, $error->getMessage(), 'The update is refused for the wrong reason');
            $this->assertSame([], self::ids($this->authorization->skip(fn (): Document => $this->database->getDocument('parents', 'parent'))->getAttribute('related')), 'A refused update links nothing to the parent');
            $stored = $this->authorization->skip(fn (): Document => $this->database->getDocument('related', 'stored'));
            $this->assertSame('Stored', $stored->getAttribute('name'), 'A refused update leaves the stored document unchanged');
            $this->assertSame([], self::ids($stored->getAttribute('back')), 'A refused update leaves no link on the stored document');

            return;
        }

        $this->assertNull($refusal, 'The update must be refused with: '.$refusal);
        $this->assertSame(['stored'], self::ids($this->authorization->skip(fn (): Document => $this->database->getDocument('parents', 'parent'))->getAttribute('related')));
        if ($type !== 'oneToOne one-way') {
            $this->assertSame(['parent'], self::ids($this->authorization->skip(fn (): Document => $this->database->getDocument('related', 'stored'))->getAttribute('back')));
        }
    }

    /**
     * Database::authorizeCreate() refuses what the create refuses, for the same reason, and writes nothing.
     *
     * @param  Closure(): Adapter  $adapter
     * @param  list<string>  $collectionPermissions
     * @param  list<string>  $documentPermissions
     */
    #[DataProvider('cases')]
    public function testAuthorizeCreateRefusesWhatTheCreateRefuses(Closure $adapter, string $type, bool $documentSecurity, array $collectionPermissions, array $documentPermissions, string $caller, string $value, ?string $refusal, bool $transaction): void
    {
        $this->seed($adapter(), $type, $documentSecurity, $collectionPermissions, $documentPermissions);

        $related = $value === self::ID ? 'stored' : new Document(['$id' => 'stored', 'name' => 'Changed']);
        $parent = new Document([
            '$id' => 'parent',
            '$permissions' => [Permission::read(Role::any())],
            'related' => \in_array($type, ['oneToMany', 'manyToMany'], true) ? [$related] : $related,
        ]);
        $given = $parent->getArrayCopy();

        $this->assertSame($refusal, $this->refusal(fn (): mixed => $this->as($caller, fn () => $transaction
            ? $this->database->withTransaction(fn () => $this->database->authorizeCreate('parents', $parent))
            : $this->database->authorizeCreate('parents', $parent))));

        $this->assertEquals($given, $parent->getArrayCopy(), 'The check leaves the document it is given unchanged');
        $this->assertWroteNothing();
    }

    /**
     * Database::authorizeUpdate() refuses what the update refuses, for the same reason, and writes nothing.
     *
     * @param  Closure(): Adapter  $adapter
     * @param  list<string>  $collectionPermissions
     * @param  list<string>  $documentPermissions
     */
    #[DataProvider('cases')]
    public function testAuthorizeUpdateRefusesWhatTheUpdateRefuses(Closure $adapter, string $type, bool $documentSecurity, array $collectionPermissions, array $documentPermissions, string $caller, string $value, ?string $refusal, bool $transaction): void
    {
        $this->seed($adapter(), $type, $documentSecurity, $collectionPermissions, $documentPermissions);
        $old = $this->authorization->skip(fn (): Document => $this->database->createDocument('parents', new Document([
            '$id' => 'parent',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
        ])));

        $related = $value === self::ID ? 'stored' : new Document(['$id' => 'stored', 'name' => 'Changed']);
        $update = new Document(['related' => \in_array($type, ['oneToMany', 'manyToMany'], true) ? [$related] : $related]);

        $this->assertSame($refusal, $this->refusal(fn (): mixed => $this->as($caller, fn () => $transaction
            ? $this->database->withTransaction(fn () => $this->database->authorizeUpdate('parents', $old, $update))
            : $this->database->authorizeUpdate('parents', $old, $update))));

        $this->assertSame([], self::ids($this->authorization->skip(fn (): Document => $this->database->getDocument('parents', 'parent'))->getAttribute('related')), 'The check links nothing');
        $stored = $this->authorization->skip(fn (): Document => $this->database->getDocument('related', 'stored'));
        $this->assertSame('Stored', $stored->getAttribute('name'), 'The check changes nothing');
        $this->assertSame([], self::ids($stored->getAttribute('back')), 'The check links nothing');
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

    private function assertWroteNothing(): void
    {
        $this->assertTrue($this->authorization->skip(fn (): bool => $this->database->getDocument('parents', 'parent')->isEmpty()), 'The check writes no parent');
        $stored = $this->authorization->skip(fn (): Document => $this->database->getDocument('related', 'stored'));
        $this->assertSame('Stored', $stored->getAttribute('name'), 'The check changes no related document');
        $this->assertSame([], self::ids($stored->getAttribute('back')), 'The check links nothing');
    }

    /**
     * @param  list<string>  $collectionPermissions
     * @param  list<string>  $documentPermissions
     */
    private function seed(Adapter $adapter, string $type, bool $documentSecurity, array $collectionPermissions, array $documentPermissions): void
    {
        $this->authorization = new Authorization();
        $this->database = (new Database($adapter, new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('nested_links')
            ->setNamespace('nested_links_'.\uniqid());
        $this->database->addHook(new Permissions());
        $this->database->addHook(new Relationships());

        $this->authorization->skip(function () use ($type, $documentSecurity, $collectionPermissions, $documentPermissions): void {
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
            $this->database->createDocument('related', new Document(['$id' => 'stored', 'name' => 'Stored', '$permissions' => $documentPermissions]));
        });
    }

    private function as(string $caller, callable $callback): mixed
    {
        if ($caller === self::KEY) {
            return $this->authorization->skip($callback);
        }

        $roles = $caller === self::GUEST
            ? [Role::any()->toString(), Role::guests()->toString()]
            : [Role::any()->toString(), Role::users()->toString(), Role::user(self::USER)->toString()];

        return $this->authorization->withRoles($roles, $callback);
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
