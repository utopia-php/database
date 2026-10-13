<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Permission;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

/**
 * read() answers a list the way its queries ask: documents through find(), or one row per group through aggregate()
 * when the queries aggregate or group. find() keeps refusing aggregate and groupBy queries.
 */
final class ReadTest extends TestCase
{
    private const string COLLECTION = 'posts';

    private Database $database;

    #[\Override]
    protected function setUp(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::user('editor')->toString());

        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $this->database
            ->setAuthorization($authorization)
            ->setDatabase('read')
            ->setNamespace('read_'.\uniqid());
        $this->database->addHook(new Permissions());
        $this->database->create();

        $this->database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'category', size: 16),
                Attribute::integer(key: 'views'),
            ],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));

        foreach ([
            'first' => ['news', 1, true],
            'second' => ['news', 2, false],
            'third' => ['sport', 3, true],
        ] as $id => [$category, $views, $editable]) {
            $this->database->createDocument(self::COLLECTION, new Document([
                Document::ID => $id,
                Document::PERMISSIONS => [
                    Permission::read(Role::any()),
                    ...($editable ? [Permission::update(Role::user('editor'))] : []),
                ],
                'category' => $category,
                'views' => $views,
            ]));
        }
    }

    public function testDocumentQueriesReadWhatFindReads(): void
    {
        $queries = [Query::greaterThan('views', 1), Query::orderDesc('views')];

        $read = $this->database->read(self::COLLECTION, $queries);

        $this->assertSame(['third', 'second'], \array_map(static fn (Document $document): string => $document->getId(), $read));
        $this->assertSame(
            \array_map(static fn (Document $document): array => $document->getArrayCopy(), $this->database->find(self::COLLECTION, $queries)),
            \array_map(static fn (Document $document): array => $document->getArrayCopy(), $read),
        );
    }

    public function testNoQueriesReadEveryDocument(): void
    {
        $this->assertSame(['first', 'second', 'third'], \array_map(static fn (Document $document): string => $document->getId(), $this->database->read(self::COLLECTION)));
    }

    public function testAggregateQueriesReadTheAggregateRowsAsDocuments(): void
    {
        $queries = [Query::count('*', 'total'), Query::sum('views')];

        $read = $this->database->read(self::COLLECTION, $queries);

        $this->assertContainsOnlyInstancesOf(Document::class, $read);
        $this->assertSame([['total' => 3, 'sum_views' => 6]], \array_map(static fn (Document $row): array => $row->getArrayCopy(), $read));
        $this->assertSame($this->database->aggregate(self::COLLECTION, $queries), \array_map(static fn (Document $row): array => $row->getArrayCopy(), $read));
    }

    public function testGroupByQueriesReadOneDocumentPerGroup(): void
    {
        $read = $this->database->read(self::COLLECTION, [Query::groupBy(['category']), Query::count('*', 'posts'), Query::orderAsc('category')]);

        $this->assertSame(
            [['posts' => 2, 'category' => 'news'], ['posts' => 1, 'category' => 'sport']],
            \array_map(static fn (Document $row): array => $row->getArrayCopy(), $read),
        );
    }

    public function testThePermissionIsPassedToFind(): void
    {
        $this->assertSame(
            ['first', 'third'],
            \array_map(static fn (Document $document): string => $document->getId(), $this->database->read(self::COLLECTION, [], PermissionType::Update)),
        );
    }

    public function testAnAggregationUnderAnotherPermissionThanReadIsRefused(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Aggregate and groupBy queries read under the read permission only');

        $this->database->read(self::COLLECTION, [Query::count()], PermissionType::Update);
    }

    public function testGroupedQueriesTellAnAggregationFromADocumentList(): void
    {
        $this->assertFalse(Query::groupByType([Query::equal('category', ['news']), Query::orderAsc('views'), Query::limit(2)])->isAggregation());
        $this->assertFalse(Query::groupByType([])->isAggregation());
        $this->assertTrue(Query::groupByType([Query::count()])->isAggregation());
        $this->assertTrue(Query::groupByType([Query::groupBy(['category'])])->isAggregation(), 'a groupBy reads one row per group without an aggregate');
    }

    public function testFindStillRefusesAggregateQueries(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('find() does not run aggregate or groupBy queries: use aggregate()');

        $this->database->find(self::COLLECTION, [Query::count()]);
    }

    public function testAQueryOfAnotherTypeIsRefused(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid query type: "string"');

        /** @phpstan-ignore argument.type */
        $this->database->read(self::COLLECTION, ['count()']);
    }
}
