<?php

namespace Tests\Unit\Relationships;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature\Upserts;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Id;
use Utopia\Database\Permission;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipType;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

use function Swoole\Coroutine\run;

/**
 * With placeholders replaced, a related document sent as a document whose ID is empty or Id::PLACEHOLDER gets a
 * generated ID at every depth a write relates, while a string 'unique()' stays a reference to the document stored
 * under it. With grants checked, a related document sent with permissions may only add permissions for roles the
 * caller holds.
 */
final class NestedPreparationTest extends TestCase
{
    private const string USER = 'u1';

    private const string FIRST = 'first';

    private const string SECOND = 'second';

    private const string THIRD = 'third';

    private const string NEXT = 'next';

    private const string PREVIOUS = 'previous';

    private const string UNHELD_REFUSAL = 'Permissions must be one of: (any, users, user:u1)';

    private const string MALFORMED = 'Wildcard permission "*" has been replaced. Use "any" instead.';

    private const string BULK_REFUSAL = 'Invalid relationship value. A bulk update takes related documents by ID while placeholders are replaced or grants are checked.';

    private Authorization $authorization;

    /**
     * @return iterable<string, array{Closure(): Adapter, RelationshipType}>
     */
    public static function types(): iterable
    {
        foreach (self::adapters() as $adapterName => [$adapter]) {
            foreach (RelationshipType::cases() as $type) {
                yield "{$adapterName}, {$type->value}" => [$adapter, $type];
            }
        }
    }

    /**
     * @return iterable<string, array{Closure(): Adapter}>
     */
    public static function adapters(): iterable
    {
        yield 'memory' => [static fn (): Adapter => new Memory()];
        yield 'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testPlaceholdersGetGeneratedIdsAtEveryDepth(Closure $adapter, RelationshipType $type): void
    {
        $database = $this->database($adapter(), $type)->setReplacePlaceholders(true);
        $grandchild = new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'grandchild']);
        $child = new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'child', self::NEXT => self::value($type, $grandchild)]);
        $root = new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'root', self::NEXT => self::value($type, $child)]);

        $created = $database->createDocument(self::FIRST, $root);

        $rootId = $this->assertGeneratedId($database, self::FIRST, 'root');
        $childId = $this->assertGeneratedId($database, self::SECOND, 'child');
        $grandchildId = $this->assertGeneratedId($database, self::THIRD, 'grandchild');
        $this->assertSame($rootId, $created->getId());
        $this->assertSame([$childId], self::ids($database->getDocument(self::FIRST, $rootId)->getAttribute(self::NEXT)));
        $this->assertSame([$grandchildId], self::ids($database->getDocument(self::SECOND, $childId)->getAttribute(self::NEXT)));
        $this->assertSame(Id::PLACEHOLDER, $child->getId(), 'The caller\'s nested document is not changed');
        $this->assertSame(Id::PLACEHOLDER, $grandchild->getId(), 'The caller\'s nested document is not changed');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAPlaceholderThreeLevelsDownGetsAGeneratedId(Closure $adapter): void
    {
        $database = $this->database($adapter(), RelationshipType::ManyToMany)->setReplacePlaceholders(true);

        $database->createDocument(self::FIRST, new Document([
            Document::ID => 'root',
            self::NEXT => [new Document([
                Document::ID => Id::PLACEHOLDER,
                'name' => 'child',
                self::NEXT => [new Document([
                    Document::ID => Id::PLACEHOLDER,
                    'name' => 'grandchild',
                    self::PREVIOUS => [new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'great-grandchild'])],
                ])],
            ])],
        ]));

        $this->assertGeneratedId($database, self::SECOND, 'child');
        $this->assertGeneratedId($database, self::SECOND, 'great-grandchild');
        $this->assertGeneratedId($database, self::THIRD, 'grandchild');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAnEmptyIdIsGeneratedAndAnExplicitIdIsKept(Closure $adapter, RelationshipType $type): void
    {
        $database = $this->database($adapter(), $type)->setReplacePlaceholders(true);

        $database->createDocument(self::FIRST, new Document([
            Document::ID => 'root',
            self::NEXT => self::value($type, new Document([
                Document::ID => 'chosen',
                'name' => 'child',
                self::NEXT => self::value($type, new Document(['name' => 'grandchild'])),
            ])),
        ]));

        $this->assertSame('child', $database->getDocument(self::SECOND, 'chosen')->getAttribute('name'));
        $this->assertGeneratedId($database, self::THIRD, 'grandchild');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAStringPlaceholderStaysAReference(Closure $adapter, RelationshipType $type): void
    {
        $database = $this->database($adapter(), $type);
        $database->createDocument(self::SECOND, new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'literal']));
        $database->setReplacePlaceholders(true);

        $database->createDocument(self::FIRST, new Document([Document::ID => 'linked', self::NEXT => self::value($type, Id::PLACEHOLDER)]));

        $this->assertSame([Id::PLACEHOLDER], self::ids($database->getDocument(self::FIRST, 'linked')->getAttribute(self::NEXT)));
        $this->assertSame(['literal'], self::names($database, self::SECOND), 'Linking by the string creates nothing');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAStringPlaceholderWithoutALiteralDocumentLinksNothing(Closure $adapter, RelationshipType $type): void
    {
        $database = $this->database($adapter(), $type)->setReplacePlaceholders(true);

        $database->createDocument(self::FIRST, new Document([Document::ID => 'linked', self::NEXT => self::value($type, Id::PLACEHOLDER)]));

        $this->assertSame([], self::ids($database->getDocument(self::FIRST, 'linked')->getAttribute(self::NEXT)));
        $this->assertSame([], self::names($database, self::SECOND));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testEveryWriteGeneratesNewIds(Closure $adapter, RelationshipType $type): void
    {
        $database = $this->database($adapter(), $type)->setReplacePlaceholders(true);
        $root = static fn (string $id): Document => new Document([
            Document::ID => $id,
            self::NEXT => self::value($type, new Document([Document::ID => Id::PLACEHOLDER, 'name' => $id])),
        ]);

        $database->createDocument(self::FIRST, $root('one'));
        $database->createDocuments(self::FIRST, [$root('two'), $root('three')]);

        $one = $this->assertGeneratedId($database, self::SECOND, 'one');
        $two = $this->assertGeneratedId($database, self::SECOND, 'two');
        $three = $this->assertGeneratedId($database, self::SECOND, 'three');
        $this->assertCount(3, \array_unique([$one, $two, $three]));
        $this->assertSame([$two], self::ids($database->getDocument(self::FIRST, 'two')->getAttribute(self::NEXT)));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testTopLevelPlaceholdersAreGenerated(Closure $adapter): void
    {
        $database = $this->database($adapter(), RelationshipType::ManyToOne)->setReplacePlaceholders(true);

        $database->createDocument(self::SECOND, new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'single']));
        $database->createDocuments(self::SECOND, [new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'batched'])]);
        if ($database->getAdapter()->hasFeature(Upserts::class)) {
            $upserted = $database->upsertDocument(self::SECOND, new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'upserted']));
            $database->upsertDocuments(self::SECOND, [new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'upserted in bulk'])]);
            $this->assertSame($upserted->getId(), $this->assertGeneratedId($database, self::SECOND, 'upserted'));
            $this->assertGeneratedId($database, self::SECOND, 'upserted in bulk');
        }

        $this->assertGeneratedId($database, self::SECOND, 'single');
        $this->assertGeneratedId($database, self::SECOND, 'batched');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAnUpdateGeneratesIdsForNestedPlaceholders(Closure $adapter, RelationshipType $type): void
    {
        $database = $this->database($adapter(), $type);
        $database->createDocument(self::FIRST, new Document([Document::ID => 'root', 'name' => 'root']));
        $database->setReplacePlaceholders(true);

        $database->updateDocument(self::FIRST, 'root', new Document([
            self::NEXT => self::value($type, new Document([
                Document::ID => Id::PLACEHOLDER,
                'name' => 'child',
                self::NEXT => self::value($type, new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'grandchild'])),
            ])),
        ]));

        $childId = $this->assertGeneratedId($database, self::SECOND, 'child');
        $this->assertGeneratedId($database, self::THIRD, 'grandchild');
        $this->assertSame([$childId], self::ids($database->getDocument(self::FIRST, 'root')->getAttribute(self::NEXT)));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAnUpsertGeneratesIdsForNestedPlaceholders(Closure $adapter, RelationshipType $type): void
    {
        $database = $this->database($adapter(), $type)->setReplacePlaceholders(true);
        if (! $database->getAdapter()->hasFeature(Upserts::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->upsertDocument(self::FIRST, new Document([
            Document::ID => 'single',
            self::NEXT => self::value($type, new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'single child'])),
        ]));
        $database->upsertDocuments(self::FIRST, [new Document([
            Document::ID => 'bulk',
            self::NEXT => self::value($type, new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'bulk child'])),
        ])]);

        $this->assertSame([$this->assertGeneratedId($database, self::SECOND, 'single child')], self::ids($database->getDocument(self::FIRST, 'single')->getAttribute(self::NEXT)));
        $this->assertSame([$this->assertGeneratedId($database, self::SECOND, 'bulk child')], self::ids($database->getDocument(self::FIRST, 'bulk')->getAttribute(self::NEXT)));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAnUnheldRoleIsRefusedAtEachDepth(Closure $adapter, RelationshipType $type): void
    {
        $database = $this->database($adapter(), $type)->setCheckGrants(true);
        $unheld = [Permission::read(Role::user('u2'))];

        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $database->createDocument(self::FIRST, new Document([
            Document::ID => 'depth one',
            self::NEXT => self::value($type, new Document(['name' => 'child', Document::PERMISSIONS => $unheld])),
        ]))));
        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $database->createDocument(self::FIRST, new Document([
            Document::ID => 'depth two',
            self::NEXT => self::value($type, new Document([
                'name' => 'child',
                self::NEXT => self::value($type, new Document(['name' => 'grandchild', Document::PERMISSIONS => $unheld])),
            ])),
        ]))));

        $this->assertWroteNothing($database);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAnUnheldRoleOnANewToManyItemIsRefused(Closure $adapter): void
    {
        foreach ([RelationshipType::OneToMany, RelationshipType::ManyToMany] as $type) {
            $database = $this->database($adapter(), $type);
            $database->createDocument(self::FIRST, new Document([Document::ID => 'root', self::NEXT => [new Document([Document::ID => 'held', 'name' => 'held'])]]));
            $database->setCheckGrants(true);

            $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $database->updateDocument(self::FIRST, 'root', new Document([
                self::NEXT => ['held', new Document(['name' => 'new', Document::PERMISSIONS => [Permission::update(Role::team('admin'))]])],
            ]))));
            $this->assertSame(['held'], self::names($database, self::SECOND), $type->value);
            $this->assertSame(['held'], self::ids($database->getDocument(self::FIRST, 'root')->getAttribute(self::NEXT)), $type->value);
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testHeldUnchangedAndClearedPermissionsAreAllowed(Closure $adapter, RelationshipType $type): void
    {
        $database = $this->database($adapter(), $type);
        $foreign = [Permission::read(Role::user('u2'))];
        $database->createDocument(self::SECOND, new Document([Document::ID => 'stored', 'name' => 'stored', Document::PERMISSIONS => [...$foreign, Permission::update(Role::any())]]));
        $database->setCheckGrants(true);

        $held = [Permission::read(Role::user(self::USER)), Permission::update(Role::users()), Permission::write(Role::any())];
        $database->createDocument(self::FIRST, new Document([Document::ID => 'held', self::NEXT => self::value($type, new Document(['name' => 'held', Document::PERMISSIONS => $held]))]));
        $database->createDocument(self::FIRST, new Document([Document::ID => 'unchanged', self::NEXT => self::value($type, new Document([Document::ID => 'stored', 'name' => 'unchanged', Document::PERMISSIONS => $foreign]))]));
        $database->createDocument(self::FIRST, new Document([Document::ID => 'cleared', self::NEXT => self::value($type, new Document(['name' => 'cleared', Document::PERMISSIONS => []]))]));

        $this->assertSame($held, $this->related($database, 'held')->getPermissions());
        $this->assertSame('unchanged', $database->getDocument(self::SECOND, 'stored')->getAttribute('name'));
        $this->assertSame([], $this->related($database, 'cleared')->getPermissions());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAbsentOrNullPermissionsInheritTheParentsUnchecked(Closure $adapter, RelationshipType $type): void
    {
        $database = $this->database($adapter(), $type)->setCheckGrants(true);
        $inherited = [Permission::read(Role::user('u2'))];
        $nulled = new Document(['name' => 'nulled']);
        $nulled->setAttribute(Document::PERMISSIONS, null);

        $database->createDocument(self::FIRST, new Document([Document::ID => 'absent', Document::PERMISSIONS => $inherited, self::NEXT => self::value($type, new Document(['name' => 'absent']))]));
        $database->createDocument(self::FIRST, new Document([Document::ID => 'nulled', Document::PERMISSIONS => $inherited, self::NEXT => self::value($type, $nulled)]));

        $this->assertSame($inherited, $this->related($database, 'absent')->getPermissions());
        $this->assertSame($inherited, $this->related($database, 'nulled')->getPermissions());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testMalformedPermissionsAreAStructureError(Closure $adapter): void
    {
        $database = $this->database($adapter(), RelationshipType::ManyToOne);
        $create = fn () => $database->createDocument(self::FIRST, new Document([Document::ID => 'root', self::NEXT => new Document(['name' => 'child', Document::PERMISSIONS => ['*']])]));

        $unchecked = $this->failure($create);
        $this->assertNotInstanceOf(StructureException::class, $unchecked);
        $this->assertInstanceOf(DatabaseException::class, $unchecked);

        $database->setCheckGrants(true);
        $checked = $this->failure($create);
        $this->assertInstanceOf(StructureException::class, $checked);
        $this->assertSame(self::MALFORMED, $checked->getMessage());
        $this->assertWroteNothing($database);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testDisabledAuthorizationOrTheToggleOffAllowsAnyRole(Closure $adapter, RelationshipType $type): void
    {
        $database = $this->database($adapter(), $type);
        $unheld = [Permission::read(Role::user('u2'))];
        $root = fn (string $id): Document => new Document([Document::ID => $id, self::NEXT => self::value($type, new Document(['name' => $id, Document::PERMISSIONS => $unheld]))]);

        $database->createDocument(self::FIRST, $root('toggle off'));
        $database->setCheckGrants(true);
        $this->authorization->skip(fn () => $database->createDocument(self::FIRST, $root('authorization disabled')));
        $database->withCheckGrants(false, fn () => $database->createDocument(self::FIRST, $root('scoped off')));

        $this->assertSame($unheld, $this->related($database, 'toggle off')->getPermissions());
        $this->assertSame($unheld, $this->related($database, 'authorization disabled')->getPermissions());
        $this->assertSame($unheld, $this->related($database, 'scoped off')->getPermissions());
        $this->assertTrue($database->isCheckingGrants());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testAPlaceholderWithAnUnheldRoleWritesNothingAndWithAHeldRoleIsCreated(Closure $adapter, RelationshipType $type): void
    {
        $database = $this->database($adapter(), $type)->setReplacePlaceholders(true)->setCheckGrants(true);
        $root = fn (string $role): Document => new Document([
            Document::ID => 'root',
            self::NEXT => self::value($type, new Document([Document::ID => Id::PLACEHOLDER, 'name' => 'child', Document::PERMISSIONS => [Permission::read(Role::parse($role))]])),
        ]);

        $this->assertSame(self::UNHELD_REFUSAL, $this->refusal(fn () => $database->createDocument(self::FIRST, $root('user:u2'))));
        $this->assertWroteNothing($database);

        $database->createDocument(self::FIRST, $root('user:'.self::USER));
        $childId = $this->assertGeneratedId($database, self::SECOND, 'child');
        $this->assertSame([Permission::read(Role::user(self::USER))], $database->getDocument(self::SECOND, $childId)->getPermissions());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('types')]
    public function testABulkUpdateRefusesARelatedDocumentGivenAsADocument(Closure $adapter, RelationshipType $type): void
    {
        $toggles = [
            'placeholders replaced' => static fn (Database $database): Database => $database->setReplacePlaceholders(true),
            'grants checked' => static fn (Database $database): Database => $database->setCheckGrants(true),
        ];

        foreach ($toggles as $toggle => $enable) {
            $database = $this->database($adapter(), $type);
            $database->createDocuments(self::FIRST, [new Document([Document::ID => 'one', 'name' => 'one']), new Document([Document::ID => 'two', 'name' => 'two'])]);
            $enable($database);
            $related = ['name' => 'child', Document::PERMISSIONS => [Permission::update(Role::team('admin'))]];
            $toMany = \in_array($type, [RelationshipType::OneToMany, RelationshipType::ManyToMany], true);
            $values = [
                'document' => self::value($type, new Document([Document::ID => Id::PLACEHOLDER, ...$related])),
                'array' => $toMany ? [$related] : $related,
            ];

            foreach ($values as $form => $value) {
                $failure = $this->failure(fn () => $database->updateDocuments(self::FIRST, new Document([self::NEXT => $value])));

                $this->assertInstanceOf(RelationshipException::class, $failure, "{$toggle}, {$form}");
                $this->assertSame(self::BULK_REFUSAL, $failure->getMessage(), "{$toggle}, {$form}");
            }

            $this->assertSame(['one', 'two'], self::names($database, self::FIRST), $toggle);
            $this->assertSame([], self::names($database, self::SECOND), $toggle);
            $this->assertSame([], self::ids($database->getDocument(self::FIRST, 'one')->getAttribute(self::NEXT)), $toggle);
            $this->assertSame([], self::ids($database->getDocument(self::FIRST, 'two')->getAttribute(self::NEXT)), $toggle);
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testABulkUpdateLinksARelatedDocumentByIdWhileBothTogglesAreOn(Closure $adapter): void
    {
        $database = $this->database($adapter(), RelationshipType::ManyToOne);
        $database->createDocument(self::SECOND, new Document([Document::ID => 'stored', 'name' => 'stored']));
        $database->createDocuments(self::FIRST, [new Document([Document::ID => 'one', 'name' => 'one']), new Document([Document::ID => 'two', 'name' => 'two'])]);
        $database->setReplacePlaceholders(true)->setCheckGrants(true);

        $this->assertSame(2, $database->updateDocuments(self::FIRST, new Document([self::NEXT => 'stored'])));

        $this->assertSame(['stored'], self::ids($database->getDocument(self::FIRST, 'one')->getAttribute(self::NEXT)));
        $this->assertSame(['stored'], self::ids($database->getDocument(self::FIRST, 'two')->getAttribute(self::NEXT)));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testTheTogglesAreScopedAndOffByDefault(Closure $adapter): void
    {
        $database = $this->database($adapter(), RelationshipType::ManyToOne);

        $toggles = static fn (): array => [$database->isReplacingPlaceholders(), $database->isCheckingGrants()];

        $this->assertSame([false, false], $toggles());
        $this->assertSame([true, true], $database->withReplacePlaceholders(true, fn (): array => $database->withCheckGrants(true, $toggles)));
        $this->assertSame([false, false], $toggles());
    }

    public function testASnapshotCarriesBothTogglesIntoAChildCoroutine(): void
    {
        if (! \extension_loaded('swoole')) {
            $this->markTestSkipped('ext-swoole is required for coroutine-scoped toggles');
        }

        $database = $this->database(new Memory(), RelationshipType::ManyToOne);
        $outcomes = null;
        $hookFlags = Runtime::getHookFlags();

        try {
            run(function () use ($database, &$outcomes): void {
                $released = new Channel(1);
                $results = new Channel(1);

                $database->withReplacePlaceholders(true, fn () => $database->withCheckGrants(true, function () use ($database, $released, $results): void {
                    $snapshot = $database->snapshot();

                    Coroutine::create(function () use ($database, $snapshot, $released, $results): void {
                        $released->pop();
                        $results->push([
                            'unscoped' => $this->refusal(fn () => $database->createDocument(self::FIRST, $this->placeholderChild('unscoped', 'user:u2'))),
                            'refused' => $database->withSnapshot($snapshot, fn (): ?string => $this->refusal(fn () => $database->createDocument(self::FIRST, $this->placeholderChild('refused', 'user:u2')))),
                            'generated' => $database->withSnapshot($snapshot, fn (): ?string => $this->refusal(fn () => $database->createDocument(self::FIRST, $this->placeholderChild('generated', 'user:'.self::USER)))),
                        ]);
                    });
                }));

                $released->push(true);
                $outcomes = $results->pop(5);
            });
        } finally {
            Runtime::setHookFlags($hookFlags);
        }

        $this->assertSame(['unscoped' => null, 'refused' => self::UNHELD_REFUSAL, 'generated' => null], $outcomes);
        $this->assertSame([Id::PLACEHOLDER], self::ids($database->getDocument(self::FIRST, 'unscoped')->getAttribute(self::NEXT)), 'Outside the snapshot both toggles are off');
        $this->assertTrue($database->getDocument(self::FIRST, 'refused')->isEmpty());
        $generated = self::ids($database->getDocument(self::FIRST, 'generated')->getAttribute(self::NEXT));
        $this->assertCount(1, $generated);
        $this->assertNotSame(Id::PLACEHOLDER, $generated[0]);
    }

    private function placeholderChild(string $id, string $role): Document
    {
        return new Document([
            Document::ID => $id,
            self::NEXT => new Document([Document::ID => Id::PLACEHOLDER, 'name' => $id, Document::PERMISSIONS => [Permission::read(Role::parse($role))]]),
        ]);
    }

    private function database(Adapter $adapter, RelationshipType $type): Database
    {
        $this->authorization = new Authorization();
        $this->authorization->addRole(Role::users()->toString());
        $this->authorization->addRole(Role::user(self::USER)->toString());

        $database = (new Database($adapter, new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('nested_preparation')
            ->setNamespace('nested_preparation_'.\uniqid());
        $database->addHook(new Permissions());
        $database->addHook(new Relationships());
        $database->create();

        foreach ([self::FIRST, self::SECOND, self::THIRD] as $collection) {
            $database->createCollection(Collection::create(
                $collection,
                attributes: [Attribute::string('name', size: 64)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())],
                documentSecurity: false,
            ));
        }

        $database->createRelationship(self::FIRST, self::relationship($type, self::SECOND));
        $database->createRelationship(self::SECOND, self::relationship($type, self::THIRD));

        return $database;
    }

    private static function relationship(RelationshipType $type, string $related): Relationship
    {
        return match ($type) {
            RelationshipType::OneToOne => Relationship::oneToOne($related, self::NEXT, onDelete: RelationshipDeleteAction::SetNull),
            RelationshipType::ManyToOne => Relationship::manyToOne($related, self::NEXT, onDelete: RelationshipDeleteAction::SetNull),
            RelationshipType::OneToMany => Relationship::oneToMany($related, self::NEXT, twoWay: true, twoWayKey: self::PREVIOUS, onDelete: RelationshipDeleteAction::SetNull),
            RelationshipType::ManyToMany => Relationship::manyToMany($related, self::NEXT, twoWay: true, twoWayKey: self::PREVIOUS, onDelete: RelationshipDeleteAction::SetNull),
        };
    }

    /**
     * @return Document|string|list<Document|string>
     */
    private static function value(RelationshipType $type, Document|string $related): Document|string|array
    {
        return \in_array($type, [RelationshipType::OneToMany, RelationshipType::ManyToMany], true) ? [$related] : $related;
    }

    /**
     * The generated ID of the only document named $name in $collection.
     */
    private function assertGeneratedId(Database $database, string $collection, string $name): string
    {
        $matches = \array_values(\array_filter(
            $this->authorization->skip(fn (): array => $database->find($collection)),
            static fn (Document $document): bool => $document->getAttribute('name') === $name,
        ));

        $this->assertCount(1, $matches, "One document named {$name} in {$collection}");
        $id = $matches[0]->getId();
        $this->assertNotSame('', $id);
        $this->assertNotSame(Id::PLACEHOLDER, $id);
        $this->assertTrue($this->authorization->skip(fn (): bool => $database->getDocument($collection, Id::PLACEHOLDER)->isEmpty()), "Nothing is stored under the placeholder in {$collection}");

        return $id;
    }

    /**
     * The first related document of the parent $id.
     */
    private function related(Database $database, string $id): Document
    {
        $related = $this->authorization->skip(fn (): Document => $database->getDocument(self::FIRST, $id))->getAttribute(self::NEXT);
        $related = \is_array($related) ? $related[0] : $related;
        $this->assertInstanceOf(Document::class, $related);

        return $this->authorization->skip(fn (): Document => $database->getDocument(self::SECOND, $related->getId()));
    }

    private function assertWroteNothing(Database $database): void
    {
        foreach ([self::FIRST, self::SECOND, self::THIRD] as $collection) {
            $this->assertSame([], self::names($database, $collection), "Nothing is written to {$collection}");
        }
    }

    /**
     * @return list<string>
     */
    private static function names(Database $database, string $collection): array
    {
        return $database->getAuthorization()->skip(fn (): array => \array_values(\array_map(
            static function (Document $document): string {
                $name = $document->getAttribute('name');

                return \is_string($name) ? $name : $document->getId();
            },
            $database->find($collection),
        )));
    }

    /**
     * @return list<string>
     */
    private static function ids(mixed $value): array
    {
        $values = \is_array($value) ? $value : ($value === null ? [] : [$value]);

        $ids = \array_map(static fn (mixed $item): string => $item instanceof Document ? $item->getId() : (\is_string($item) ? $item : ''), $values);

        return \array_values(\array_filter($ids, static fn (string $id): bool => $id !== ''));
    }

    private function refusal(callable $write): ?string
    {
        try {
            $write();
        } catch (AuthorizationException $error) {
            return $error->getMessage();
        }

        return null;
    }

    private function failure(callable $write): ?Throwable
    {
        try {
            $write();
        } catch (Throwable $error) {
            return $error;
        }

        return null;
    }
}
