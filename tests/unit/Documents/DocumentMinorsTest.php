<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;
use Utopia\Query\Schema\IndexType;

final class DocumentMinorsTest extends TestCase
{
    private const string COLLECTION = 'entries';

    private int $calls = 0;

    public function testNumericStringChangesKeepTheirNumberType(): void
    {
        $database = $this->database(new Memory());
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'entry', 'ratio' => 10.0, 'count' => 1]));

        $this->assertSame(11.5, $database->increaseDocumentAttribute(self::COLLECTION, 'entry', 'ratio', '1.5')->getAttribute('ratio'));
        $this->assertSame(13.5, $database->increaseDocumentAttribute(self::COLLECTION, 'entry', 'ratio', '2e0')->getAttribute('ratio'));
        $this->assertSame(10.5, $database->decreaseDocumentAttribute(self::COLLECTION, 'entry', 'ratio', '3')->getAttribute('ratio'));

        $schemaless = $this->database($this->without(Capability::DefinedAttributes));
        $schemaless->createDocument(self::COLLECTION, new Document([Document::ID => 'entry']));
        $this->assertSame(3, $schemaless->increaseDocumentAttribute(self::COLLECTION, 'entry', 'hits', '3')->getAttribute('hits'));
        $this->assertSame(4.5, $schemaless->increaseDocumentAttribute(self::COLLECTION, 'entry', 'hits', '1.5')->getAttribute('hits'));
    }

    public function testANonNumericBoundIsRefused(): void
    {
        $database = $this->database(new Memory());
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'entry', 'ratio' => 10.0]));

        $this->assertThrows(TypeException::class, 'Value must be numeric.', fn (): Document => $database->increaseDocumentAttribute(self::COLLECTION, 'entry', 'ratio', 1, 'plenty'));
        $this->assertSame(10.0, $database->getDocument(self::COLLECTION, 'entry')->getAttribute('ratio'));
    }

    public function testATimeToLiveIndexWithoutAPeriodIsRefused(): void
    {
        $this->assertThrows(
            IndexException::class,
            'TTL must be at least 1 second',
            fn (): Index => Index::fromArray(['key' => 'expiry', 'type' => IndexType::Ttl, 'attributes' => ['recordedAt'], 'ttl' => 0]),
        );
    }

    public function testAnUnchangedListOfRelatedIdsIsNotAChange(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database = new class (new Memory(), new Cache(new None())) extends Database {
            public function getDocument(string $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                $document = parent::getDocument($collection, $id, $queries, $forUpdate);
                if ($forUpdate && $collection === 'parents') {
                    /** @var array<Document|string> $children */
                    $children = $document->getAttribute('children', []);
                    $document->setAttribute('children', \array_map(
                        static fn (Document|string $child): string => $child instanceof Document ? $child->getId() : $child,
                        $children,
                    ));
                }

                return $document;
            }
        };
        $database->setAuthorization($authorization)->setDatabase('minors')->setNamespace('minors_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships());
        $readOnly = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(id: 'parents', permissions: $readOnly));
        $database->createCollection(Collection::create(id: 'children', permissions: [...$readOnly, Permission::update(Role::any())]));
        $database->createRelationship('parents', Relationship::oneToMany(relatedCollection: 'children', twoWay: true, key: 'children', twoWayKey: 'parent'));
        $database->createDocument('children', new Document([Document::ID => 'c1']));
        $database->createDocument('children', new Document([Document::ID => 'c2']));
        $database->createDocument('parents', new Document([Document::ID => 'p1', 'children' => ['c1', 'c2']]));

        $updated = $database->updateDocument('parents', 'p1', new Document(['children' => ['c1', 'c2']]));

        $this->assertSame('p1', $updated->getId(), 'an unchanged list needs no update permission');
    }

    public function testBulkDeleteGuards(): void
    {
        $database = $this->database(new Memory());
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'entry', 'count' => 1]));

        $this->assertThrows(DatabaseException::class, 'Collection not found', fn (): int => $database->deleteDocuments('missing'));
        $this->assertThrows(QueryException::class, 'Invalid query: Attribute not found in schema: missing', fn (): int => $database->deleteDocuments(self::COLLECTION, [Query::equal('missing', ['x'])]));
        $this->assertThrows(DatabaseException::class, 'Cursor document must be from the same Collection.', fn (): int => $database->deleteDocuments(self::COLLECTION, [Query::cursorAfter(new Document([Document::ID => 'entry', Document::COLLECTION => 'other']))]));

        $collection = $database->getCollection(self::COLLECTION);
        $stored = $database->getAdapter()->getDocument($collection, 'entry');
        $stored->setAttribute(Document::UPDATED_AT, 'not-a-date');
        $database->getAdapter()->updateDocument($collection, 'entry', $stored, true);
        $this->assertThrows(DatabaseException::class, null, fn (): int => $database->deleteDocuments(self::COLLECTION));

        $this->assertSame(1, $database->count(self::COLLECTION));
    }

    public function testAWriteWhoseCacheOwnerCannotBeRegisteredIsRolledBack(): void
    {
        $cache = new class () extends MemoryCache {
            public bool $refuseOwners = false;

            /**
             * @param  array<int|string, mixed>|string  $data
             * @return bool|string|array<int|string, mixed>
             */
            public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                if ($this->refuseOwners && \str_contains($key, '#owner')) {
                    return false;
                }

                return parent::save($key, $data, $hash);
            }
        };
        $database = $this->database(new Memory(), new Cache($cache));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'entry', 'count' => 1]));
        $database->getDocument(self::COLLECTION, 'entry');
        $cache->refuseOwners = true;

        $this->assertThrows(RuntimeException::class, null, fn (): int => $database->updateDocuments(self::COLLECTION, new Document(['count' => 2])));
        $cache->refuseOwners = false;

        $this->assertSame(1, $database->getDocument(self::COLLECTION, 'entry')->getAttribute('count'));
    }

    public function testMixedOrNestedDocumentListsAreNotCached(): void
    {
        $database = $this->database(new Memory(), new Cache(new MemoryCache()));
        $entry = $database->createDocument(self::COLLECTION, new Document([Document::ID => 'entry', 'count' => 1]));
        $foreign = new Document([Document::ID => 'foreign', Document::COLLECTION => 'other']);
        $loose = new Document([Document::ID => 'loose']);

        foreach ([
            'documents of two collections' => [$entry, $foreign],
            'a document then a value' => [$entry, 'x'],
            'a value then a document' => ['x', $entry],
            'a document without a collection' => [$loose],
            'a document inside a list' => [[$entry]],
            'a document deep inside a list' => ['x', ['y', [$entry]]],
        ] as $case => $value) {
            $this->calls = 0;
            $key = 'minors:'.\md5($case);
            $database->withCache($key, fn (): array => $this->tally($value));
            $database->withCache($key, fn (): array => $this->tally($value));
            $this->assertSame(2, $this->calls, "{$case} is computed every time");
        }

        $this->calls = 0;
        $database->withCache('minors:plain', fn (): array => $this->tally(['x', ['y', 'z']]));
        $this->assertSame(['x', ['y', 'z']], $database->withCache('minors:plain', fn (): array => $this->tally(['other'])));
        $this->assertSame(1, $this->calls, 'a list of plain values is cached');
    }

    public function testCountAndSumWithARelationshipFilterThatMatchesNothingAreZero(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database = new Database(new Memory(), new Cache(new None()));
        $database->setAuthorization($authorization)->setDatabase('minors')->setNamespace('minors_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships());
        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(id: 'books', attributes: [Attribute::integer(key: 'pages')], permissions: $permissions));
        $database->createCollection(Collection::create(id: 'authors', attributes: [Attribute::string(key: 'name', size: 32)], permissions: $permissions));
        $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));
        $database->createDocument('authors', new Document([Document::ID => 'ada', 'name' => 'Ada']));
        $database->createDocument('books', new Document([Document::ID => 'notes', 'pages' => 120, 'author' => 'ada']));

        $this->assertSame(1, $database->count('books', [Query::equal('author.name', ['Ada'])]));
        $this->assertSame(120, $database->sum('books', 'pages', [Query::equal('author.name', ['Ada'])]));
        $this->assertSame(0, $database->count('books', [Query::equal('author.name', ['Nobody'])]));
        $this->assertSame(0, $database->sum('books', 'pages', [Query::equal('author.name', ['Nobody'])]));
    }

    public function testAJoinOnAMissingCollectionIsRefused(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        $join = Query::join('missing', 'gone', [Query::on('count', 'count')]);

        foreach ([
            fn (): mixed => $database->find(self::COLLECTION, [$join]),
            fn (): mixed => $database->count(self::COLLECTION, [$join]),
        ] as $read) {
            $this->assertThrows(QueryException::class, "Joined collection 'missing' not found", fn (): mixed => $database->skipValidation($read));
        }
    }

    public function testSelectionsMustNameDeclaredAttributes(): void
    {
        $database = $this->database(new Memory());
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'entry', 'count' => 1]));

        $this->assertThrows(QueryException::class, 'Cannot select attributes: unknown', fn (): mixed => $database->skipValidation(
            fn (): array => $database->find(self::COLLECTION, [Query::select(['count', 'unknown'])]),
        ));
        $this->assertThrows(QueryException::class, 'Select queries must contain only string attributes.', fn (): mixed => $database->skipValidation(
            fn (): array => $database->find(self::COLLECTION, [new Query(Method::Select, '', ['count', 5])]),
        ));
        $this->assertSame(1, $database->skipValidation(fn (): array => $database->find(self::COLLECTION, [Query::select(['count'])]))[0]->getAttribute('count'));
    }

    /**
     * @param  array<mixed>  $value
     * @return array<mixed>
     */
    private function tally(array $value): array
    {
        $this->calls++;

        return $value;
    }

    private function without(Capability $missing): Memory
    {
        return new class ($missing) extends Memory {
            public function __construct(private readonly Capability $missing)
            {
                parent::__construct();
            }

            public function capabilities(): array
            {
                return \array_values(\array_filter(
                    parent::capabilities(),
                    fn (Capability $capability): bool => $capability !== $this->missing,
                ));
            }
        };
    }

    /**
     * @param  class-string<Throwable>  $exception
     * @param  callable(): mixed  $operation
     */
    private function assertThrows(string $exception, ?string $message, callable $operation): void
    {
        $error = null;
        try {
            $operation();
        } catch (Throwable $caught) {
            $error = $caught;
        }

        $this->assertInstanceOf($exception, $error);
        if ($message !== null) {
            $this->assertSame($message, $error->getMessage());
        }
    }

    private function database(Adapter $adapter, ?Cache $cache = null): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database = new Database($adapter, $cache ?? new Cache(new None()));
        $database->setAuthorization($authorization)->setDatabase('minors')->setNamespace('minors_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::integer(key: 'count'), Attribute::float(key: 'ratio'), Attribute::datetime(key: 'recordedAt')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())],
        ));

        return $database;
    }
}
