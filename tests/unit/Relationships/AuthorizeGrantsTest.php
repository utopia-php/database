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
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Structure as StructureException;
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
use Utopia\Database\Validator\Permissions as PermissionsValidator;

/**
 * Database::authorizeGrants() runs only the grant check of authorizeCreate() and authorizeUpdate(): a related
 * document given as a document may only add permissions for roles the caller holds, judged after the Hook\Nested
 * hooks and, with a resolver, by its staged view. It checks no create or update permission and writes nothing.
 */
final class AuthorizeGrantsTest extends TestCase
{
    private const string USER = 'u1';

    private const string UNHELD = 'Permissions must be one of: (any, users, user:u1)';

    private const string NO_CREATE = "No permissions provided for action 'create'";

    private const string NO_UPDATE = "No permissions provided for action 'update'";

    private const string PARENTS = 'parents';

    private const string PARENT = 'parent';

    private const string RELATED = 'related';

    private const string LEAVES = 'leaves';

    private const string STORED = 'stored';

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
    public function testAnUnheldGrantIsReportedWhenTheParentPermissionIsMissing(Closure $adapter, RelationshipType $type): void
    {
        $this->build($adapter(), $type, [Permission::read(Role::any())], self::everything());
        $old = $this->storedParent();
        $this->database->setCheckGrants(true);
        $related = new Document(['name' => 'new', Document::PERMISSIONS => [Permission::read(Role::user('u2'))]]);
        $document = $this->parent($type, $related);
        $changes = new Document([self::RELATED => self::value($type, $related)]);

        $this->assertSame(self::NO_CREATE, $this->refusal(fn () => $this->database->authorizeCreate(self::PARENTS, $document)), 'authorizeCreate() refuses the parent first');
        $this->assertSame(self::NO_UPDATE, $this->refusal(fn () => $this->database->authorizeUpdate(self::PARENTS, $old, $changes)), 'authorizeUpdate() refuses the parent first');
        $this->assertSame(self::UNHELD, $this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $document)));
        $this->assertSame(self::UNHELD, $this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $changes)));
        $this->assertSame([], $this->relatedNames());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testItRefusesTheGrantsAuthorizeCreateAndUpdateRefuse(Closure $adapter, RelationshipType $type): void
    {
        $foreign = [Permission::read(Role::user('u2'))];
        $this->build($adapter(), $type, self::everything(), self::everything());
        $this->store(self::RELATED, self::STORED, $foreign);
        $old = $this->storedParent();
        $this->database->setCheckGrants(true);
        $malformed = new PermissionsValidator();
        $malformed->isValid(['malformed']);

        $cases = [
            'a new document adding an unheld role' => [new Document(['name' => 'new', Document::PERMISSIONS => $foreign]), self::UNHELD],
            'a new document adding held roles' => [new Document(['name' => 'new', Document::PERMISSIONS => [Permission::read(Role::user(self::USER)), Permission::update(Role::users())]]), null],
            'a new document given as an array' => [['name' => 'new', Document::PERMISSIONS => $foreign], self::UNHELD],
            'a new document without permissions' => [new Document(['name' => 'new']), null],
            'a stored document sending its roles back' => [new Document([Document::ID => self::STORED, 'name' => 'Stored', Document::PERMISSIONS => $foreign]), null],
            'a stored document adding an unheld role' => [new Document([Document::ID => self::STORED, 'name' => 'Stored', Document::PERMISSIONS => [...$foreign, Permission::update(Role::user('u2'))]]), self::UNHELD],
            'a stored document dropping its roles' => [new Document([Document::ID => self::STORED, 'name' => 'Stored', Document::PERMISSIONS => []]), null],
            'malformed permissions' => [new Document(['name' => 'new', Document::PERMISSIONS => ['malformed']]), $malformed->getDescription()],
        ];

        foreach ($cases as $case => [$related, $expected]) {
            $document = $this->parent($type, $related);
            $changes = new Document([self::RELATED => self::value($type, $related)]);
            $create = $this->refusal(fn () => $this->database->authorizeCreate(self::PARENTS, $document));
            $update = $this->refusal(fn () => $this->database->authorizeUpdate(self::PARENTS, $old, $changes));

            $this->assertSame($expected, $create, "{$case}: authorizeCreate()");
            $this->assertSame($expected, $update, "{$case}: authorizeUpdate()");
            $this->assertSame($create, $this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $document)), "{$case}: as authorizeCreate()");
            $this->assertSame($update, $this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $changes)), "{$case}: as authorizeUpdate()");
        }

        $this->expectException(StructureException::class);
        $this->database->authorizeGrants(self::PARENTS, $this->parent($type, new Document([Document::PERMISSIONS => ['malformed']])));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testItChecksNoCreateOrUpdatePermission(Closure $adapter, RelationshipType $type): void
    {
        $this->build($adapter(), $type, [], []);
        $this->store(self::RELATED, self::STORED, []);
        $this->database->setCheckGrants(true);
        $held = [Permission::read(Role::user(self::USER))];
        $documents = [
            'a new related document' => $this->parent($type, new Document(['name' => 'new', Document::PERMISSIONS => $held])),
            'a stored related document the caller cannot read' => $this->parent($type, new Document([Document::ID => self::STORED, 'name' => 'Changed', Document::PERMISSIONS => []])),
            'a related document given by ID' => $this->parent($type, self::STORED),
            'unheld roles on the parent itself' => new Document([Document::ID => self::PARENT, Document::PERMISSIONS => [Permission::read(Role::user('u2'))]]),
        ];

        foreach ($documents as $case => $document) {
            $this->assertSame(self::NO_CREATE, $this->refusal(fn () => $this->database->authorizeCreate(self::PARENTS, $document)), $case);
            $this->assertNull($this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $document)), $case);
        }

        $this->assertSame([], $this->relatedNames());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAStagedViewIsJudgedAsAuthorizeCreateJudgesIt(Closure $adapter, RelationshipType $type): void
    {
        $foreign = [Permission::read(Role::user('u2'))];
        $this->build($adapter(), $type, self::everything(), self::everything());
        $this->store(self::RELATED, self::STORED, $foreign);
        $this->database->setCheckGrants(true);
        $document = $this->parent($type, new Document([Document::ID => 'staged', 'name' => 'Staged', Document::PERMISSIONS => $foreign]));
        $staged = function (string $collection, string $id) use ($foreign): ?Document {
            $this->resolved[] = [$collection, $id];

            return $id === 'staged'
                ? new Document([Document::ID => 'staged', Document::COLLECTION => self::RELATED, 'name' => 'Staged', Document::PERMISSIONS => $foreign])
                : null;
        };
        $recreated = $this->parent($type, new Document([Document::ID => self::STORED, 'name' => 'Recreated', Document::PERMISSIONS => $foreign]));
        $deleted = static fn (string $collection, string $id): ?Document => $id === self::STORED ? new Document() : null;

        $this->assertSame(self::UNHELD, $this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $document)), 'Judged as stored, the document is new');
        $this->assertNull($this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $document, $staged)), 'Judged as staged, the role is sent back unchanged');
        $this->assertSame([[self::RELATED, 'staged']], $this->resolved);
        $this->assertNull($this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $recreated)), 'Judged as stored, the role is sent back unchanged');
        $this->assertSame(self::UNHELD, $this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $recreated, $deleted)), 'Staged for deletion, the document is new');

        foreach ([[$document, null], [$document, $staged], [$recreated, null], [$recreated, $deleted]] as [$checked, $resolve]) {
            $this->assertSame(
                $this->refusal(fn () => $this->database->authorizeCreate(self::PARENTS, $checked, $resolve)),
                $this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $checked, $resolve)),
            );
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testNestedHooksPrepareEveryRelatedDocumentFirst(Closure $adapter, RelationshipType $type): void
    {
        $foreign = [Permission::read(Role::user('u2'))];
        $this->build($adapter(), $type, self::everything(), self::everything(), chained: true);
        $this->store(self::RELATED, self::MARKER, $foreign);
        $this->store(self::LEAVES, self::MARKER, $foreign);
        $leaf = new Document([Document::ID => self::MARKER, 'name' => 'leaf', Document::PERMISSIONS => $foreign]);
        $related = new Document([Document::ID => self::MARKER, 'name' => 'related', Document::PERMISSIONS => $foreign, self::NEXT => self::value($type, $leaf)]);
        $document = $this->parent($type, $related);
        $this->database->setCheckGrants(true);

        $this->assertNull($this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $document)), 'Judged as the stored marked documents, the roles are sent back unchanged');

        $this->database->addHook($this->marker());

        $this->assertSame(self::UNHELD, $this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $document, $this->resolveAsStored(...))), 'Judged after the rewrite, the documents are new');
        $this->assertSame([[self::RELATED, self::MARKER]], $this->seen, 'The parent is prepared, then refused before its children');
        $this->assertSame([], $this->resolved, 'A rewritten ID is not resolved');
        $this->assertSame(
            $this->refusal(fn () => $this->database->authorizeCreate(self::PARENTS, $document)),
            $this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $document)),
        );

        $this->seen = [];
        $held = new Document([Document::ID => self::MARKER, 'name' => 'related', self::NEXT => self::value($type, $leaf)]);
        $this->assertSame(self::UNHELD, $this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $this->parent($type, $held))), 'A grant is checked as deep as the write relates');
        $this->assertSame([[self::RELATED, self::MARKER], [self::LEAVES, self::MARKER]], $this->seen);

        $this->seen = [];
        $leafless = new Document([Document::ID => self::MARKER, 'name' => 'related', self::NEXT => self::value($type, new Document([Document::ID => self::MARKER, 'name' => 'leaf']))]);
        $this->assertNull($this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $this->parent($type, $leafless), $this->resolveAsStored(...))));
        $this->assertSame([[self::RELATED, self::MARKER], [self::LEAVES, self::MARKER]], $this->seen);
        $this->assertSame(self::MARKER, $related->getId(), 'The document checked is not changed');
        $this->assertSame(self::MARKER, $leaf->getId());
        $this->assertSame([self::MARKER], self::ids($document->getAttribute(self::RELATED)));
        $this->assertSame([], $this->relatedNames());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAnIdANestedHookAssignsIsJudgedNew(Closure $adapter, RelationshipType $type): void
    {
        $foreign = [Permission::read(Role::user('u2'))];
        $this->build($adapter(), $type, self::everything(), self::everything());
        $this->store(self::RELATED, self::STORED, $foreign);
        $this->database->setCheckGrants(true)->addHook($this->assigning(self::STORED));
        $staged = static fn (string $collection, string $id): ?Document => $id === self::STORED
            ? new Document([Document::ID => self::STORED, Document::COLLECTION => self::RELATED, 'name' => 'Stored', Document::PERMISSIONS => $foreign])
            : null;
        $kept = $this->parent($type, new Document([Document::ID => self::STORED, 'name' => 'Stored', Document::PERMISSIONS => $foreign]));
        $assigned = $this->parent($type, new Document([Document::ID => self::MARKER, 'name' => 'Stored', Document::PERMISSIONS => $foreign]));

        $this->assertNull($this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $kept)));
        $this->assertNull($this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $kept, $staged)));
        $this->assertSame(self::UNHELD, $this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $assigned)));
        $this->assertSame(self::UNHELD, $this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $assigned, $staged)));
        $this->assertSame(
            $this->refusal(fn () => $this->database->authorizeCreate(self::PARENTS, $assigned, $staged)),
            $this->refusal(fn () => $this->database->authorizeGrants(self::PARENTS, $assigned, $staged)),
        );
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testItDoesNothingUnlessGrantsAreChecked(Closure $adapter, RelationshipType $type): void
    {
        $this->build($adapter(), $type, self::everything(), self::everything());
        $this->database->addHook($this->marker());
        $document = $this->parent($type, new Document([Document::ID => self::MARKER, 'name' => 'new', Document::PERMISSIONS => [Permission::read(Role::user('u2'))]]));
        $check = fn (string $collection = self::PARENTS) => $this->refusal(fn () => $this->database->authorizeGrants($collection, $document, $this->resolveAsStored(...)));

        $this->assertNull($check(), 'Grants are not checked by default');
        $this->assertNull($check('missing'), 'Nothing is read while grants are not checked');

        $this->database->setCheckGrants(true);

        $this->assertNull($this->database->withCheckGrants(false, $check));
        $this->assertNull($this->authorization->skip($check), 'Nothing is checked while authorization is disabled');
        $this->assertNull($this->database->skipRelationships($check), 'Nothing is checked while the relationship hook is disabled');
        $this->assertSame([], $this->seen, 'No hook runs while nothing is checked');
        $this->assertSame([], $this->resolved);
        $this->assertSame(self::UNHELD, $check());

        $this->expectException(NotFoundException::class);
        $check('missing');
    }

    /**
     * @return list<string>
     */
    private static function everything(): array
    {
        return [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())];
    }

    /**
     * Parents relate RELATED by RELATED; with $chained, RELATED relate LEAVES by NEXT, each related back by BACK.
     *
     * @param  list<string>  $parentPermissions
     * @param  list<string>  $relatedPermissions  The permissions of the RELATED and LEAVES collections
     */
    private function build(Adapter $adapter, RelationshipType $type, array $parentPermissions, array $relatedPermissions, bool $chained = false): void
    {
        $this->authorization = new Authorization();
        $this->authorization->addRole(Role::users()->toString());
        $this->authorization->addRole(Role::user(self::USER)->toString());
        $this->database = (new Database($adapter, new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('authorize_grants')
            ->setNamespace('authorize_grants_'.\uniqid());
        $this->database->addHook(new Permissions());
        $this->database->addHook(new Relationships());

        $this->authorization->skip(function () use ($type, $parentPermissions, $relatedPermissions, $chained): void {
            $this->database->create();
            $this->database->createCollection(Collection::create(
                self::PARENTS,
                attributes: [Attribute::string('name', size: 64)],
                permissions: $parentPermissions,
            ));
            foreach ($chained ? [self::RELATED, self::LEAVES] : [self::RELATED] as $collection) {
                $this->database->createCollection(Collection::create(
                    $collection,
                    attributes: [Attribute::string('name', size: 64)],
                    permissions: $relatedPermissions,
                    documentSecurity: false,
                ));
            }
            $this->database->createRelationship(self::PARENTS, self::relationship($type, self::RELATED, self::RELATED));
            if ($chained) {
                $this->database->createRelationship(self::RELATED, self::relationship($type, self::LEAVES, self::NEXT));
            }
        });
    }

    private static function relationship(RelationshipType $type, string $related, string $key): Relationship
    {
        return match ($type) {
            RelationshipType::OneToOne => Relationship::oneToOne($related, $key, twoWay: true, twoWayKey: self::BACK, onDelete: RelationshipDeleteAction::SetNull),
            RelationshipType::ManyToOne => Relationship::manyToOne($related, $key, twoWay: true, twoWayKey: self::BACK, onDelete: RelationshipDeleteAction::SetNull),
            RelationshipType::OneToMany => Relationship::oneToMany($related, $key, twoWay: true, twoWayKey: self::BACK, onDelete: RelationshipDeleteAction::SetNull),
            RelationshipType::ManyToMany => Relationship::manyToMany($related, $key, twoWay: true, twoWayKey: self::BACK, onDelete: RelationshipDeleteAction::SetNull),
        };
    }

    /**
     * @param  list<string>  $permissions
     */
    private function store(string $collection, string $id, array $permissions): void
    {
        $this->authorization->skip(fn () => $this->database->createDocument($collection, new Document([
            Document::ID => $id,
            'name' => 'Stored',
            Document::PERMISSIONS => $permissions,
        ])));
    }

    private function storedParent(): Document
    {
        return $this->authorization->skip(fn (): Document => $this->database->createDocument(self::PARENTS, new Document([
            Document::ID => self::PARENT,
            Document::PERMISSIONS => [Permission::read(Role::any())],
        ])));
    }

    /**
     * @param  Document|array<string, mixed>|string  $related
     */
    private function parent(RelationshipType $type, Document|array|string $related): Document
    {
        return new Document([
            Document::ID => self::PARENT,
            Document::PERMISSIONS => [Permission::read(Role::any())],
            self::RELATED => self::value($type, $related),
        ]);
    }

    /**
     * @param  Document|array<string, mixed>|string  $related
     * @return Document|array<string, mixed>|string|list<Document|array<string, mixed>|string>
     */
    private static function value(RelationshipType $type, Document|array|string $related): Document|array|string
    {
        return \in_array($type, [RelationshipType::OneToMany, RelationshipType::ManyToMany], true) ? [$related] : $related;
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

    /**
     * The message of the authorization or structure exception $check throws, or null when it throws none.
     */
    private function refusal(callable $check): ?string
    {
        try {
            $check();
        } catch (AuthorizationException|StructureException $error) {
            return $error->getMessage();
        }

        return null;
    }
}
