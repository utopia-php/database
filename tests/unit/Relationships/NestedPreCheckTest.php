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
use Utopia\Database\Id;
use Utopia\Database\Permission;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipType;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

/**
 * Database::authorizeCreate() and authorizeUpdate() prepare the related documents they are given as the write
 * would: a placeholder ID is generated, so the related document is judged new, and with grants checked a related
 * document may only add permissions for roles the caller holds, its current permissions being those of the staged
 * view when a resolver gives one.
 */
final class NestedPreCheckTest extends TestCase
{
    private const string USER = 'u1';

    private const string UNHELD_REFUSAL = 'Permissions must be one of: (any, users, user:u1)';

    private const string NO_CREATE = "No permissions provided for action 'create'";

    private const string RELATED = 'related';

    private const string STORED = 'stored';

    private Authorization $authorization;

    private Database $database;

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
    public function testAPlaceholderIsJudgedNewWithItsGrantsChecked(Closure $adapter, RelationshipType $type): void
    {
        $this->seed($adapter(), $type, [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())], null);
        $this->database->setReplacePlaceholders(true)->setCheckGrants(true);
        $placeholder = static fn (string $role): Document => new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'new', Document::PERMISSIONS => [Permission::read(Role::parse($role))]]);
        $parent = $this->parent($type, $placeholder('user:u2'));

        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)));
        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $this->database->authorizeUpdate('parents', $this->storedParent(), new Document([self::RELATED => $parent->getAttribute(self::RELATED)]))));
        $this->assertNull($this->refusal(fn () => $this->database->authorizeCreate('parents', $this->parent($type, $placeholder('user:'.self::USER)))));
        $this->assertSame([], $this->relatedNames());
        $this->assertSame([Id::PLACEHOLDER], self::ids($parent->getAttribute(self::RELATED)), 'The document checked is not changed');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAPlaceholderNeedsCreateWhereALiteralDocumentNeedsUpdate(Closure $adapter, RelationshipType $type): void
    {
        $this->seed($adapter(), $type, [Permission::read(Role::any()), Permission::update(Role::any())], null);
        $this->authorization->skip(fn () => $this->database->createDocument(self::RELATED, new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'Literal'])));
        $parent = $this->parent($type, new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'Changed']));

        $this->assertNull($this->refusal(fn () => $this->database->authorizeCreate('parents', $parent)), 'The literal document is linked and changed, which needs update');
        $this->assertSame(self::NO_CREATE, $this->refusal(fn () => $this->database->withReplacePlaceholders(true, fn () => $this->database->authorizeCreate('parents', $parent))), 'A generated ID is new, which needs create');
    }

    /**
     * @param  list<string>  $collectionPermissions
     * @param  list<string>|null  $storedPermissions  The related document's permissions; null stores none
     */
    private function seed(Adapter $adapter, RelationshipType $type, array $collectionPermissions, ?array $storedPermissions): void
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

        $this->authorization->skip(function () use ($type, $collectionPermissions, $storedPermissions): void {
            $this->database->create();
            $this->database->createCollection(Collection::create(
                'parents',
                attributes: [Attribute::string('name', size: 64)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            ));
            $this->database->createCollection(Collection::create(
                self::RELATED,
                attributes: [Attribute::string('name', size: 64)],
                permissions: $collectionPermissions,
                documentSecurity: false,
            ));
            $this->database->createRelationship('parents', match ($type) {
                RelationshipType::OneToOne => Relationship::oneToOne(self::RELATED, self::RELATED, onDelete: RelationshipDeleteAction::SetNull),
                RelationshipType::ManyToOne => Relationship::manyToOne(self::RELATED, self::RELATED, onDelete: RelationshipDeleteAction::SetNull),
                RelationshipType::OneToMany => Relationship::oneToMany(self::RELATED, self::RELATED, twoWay: true, twoWayKey: 'back', onDelete: RelationshipDeleteAction::SetNull),
                RelationshipType::ManyToMany => Relationship::manyToMany(self::RELATED, self::RELATED, twoWay: true, twoWayKey: 'back', onDelete: RelationshipDeleteAction::SetNull),
            });
            if ($storedPermissions !== null) {
                $this->database->createDocument(self::RELATED, new Document([Document::ID => self::STORED, 'name' => 'Stored', Document::PERMISSIONS => $storedPermissions]));
            }
        });
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
