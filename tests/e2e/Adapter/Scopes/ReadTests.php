<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Permission;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Role;

/**
 * read() answers a list the way its queries ask: the documents find() reads, or one row per group, as a document,
 * when the queries aggregate or group. find() keeps refusing aggregate and groupBy queries.
 */
trait ReadTests
{
    private const string READ_POSTS = 'read_posts';

    public function testReadReturnsTheDocumentsFindReturns(): void
    {
        $database = $this->getDatabase();
        $this->seedReadPosts($database);

        try {
            $queries = [Query::greaterThan('views', 1), Query::orderDesc('views')];
            $read = $database->read(self::READ_POSTS, $queries);

            $this->assertSame(['third', 'second'], \array_map(static fn (Document $document): string => $document->getId(), $read));
            $this->assertSame(
                \array_map(static fn (Document $document): array => $document->getArrayCopy(), $database->find(self::READ_POSTS, $queries)),
                \array_map(static fn (Document $document): array => $document->getArrayCopy(), $read),
            );
        } finally {
            $this->deleteReadPosts($database);
        }
    }

    public function testReadUnderAnotherPermissionReadsTheDocumentsHeldUnderIt(): void
    {
        $database = $this->getDatabase();
        $this->seedReadPosts($database);

        try {
            $this->assertSame(
                ['first', 'third'],
                \array_map(static fn (Document $document): string => $document->getId(), $database->read(self::READ_POSTS, [Query::orderAsc('views')], PermissionType::Update)),
            );
        } finally {
            $this->deleteReadPosts($database);
        }
    }

    public function testReadReturnsAggregateRowsAsDocuments(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->seedReadPosts($database);

        try {
            $totals = $database->read(self::READ_POSTS, [Query::count('*', 'posts'), Query::sum('views', 'views')]);
            $this->assertCount(1, $totals);
            $this->assertSame([3, 6], [self::readInteger($totals[0]->getAttribute('posts')), self::readInteger($totals[0]->getAttribute('views'))]);

            $groups = $database->read(self::READ_POSTS, [Query::groupBy(['category']), Query::count('*', 'posts'), Query::orderAsc('category')]);
            $this->assertSame(
                [['news', 2], ['sport', 1]],
                \array_map(static fn (Document $row): array => [$row->getAttribute('category'), self::readInteger($row->getAttribute('posts'))], $groups),
            );
        } finally {
            $this->deleteReadPosts($database);
        }
    }

    public function testReadRefusesAnAggregationUnderAnotherPermissionThanRead(): void
    {
        $database = $this->getDatabase();
        $this->seedReadPosts($database);

        try {
            $database->read(self::READ_POSTS, [Query::count()], PermissionType::Update);
            $this->fail('An aggregation under the update permission must be refused');
        } catch (QueryException $exception) {
            $this->assertStringContainsString('read under the read permission only', $exception->getMessage());
        } finally {
            $this->deleteReadPosts($database);
        }
    }

    public function testFindStillRefusesAnAggregation(): void
    {
        $database = $this->getDatabase();
        $this->seedReadPosts($database);

        $this->expectException(QueryException::class);
        if ($database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectExceptionMessage('find() does not run aggregate or groupBy queries: use aggregate()');
        }

        try {
            $database->find(self::READ_POSTS, [Query::count()]);
        } finally {
            $this->deleteReadPosts($database);
        }
    }

    /**
     * Posts first (news, 1 view), second (news, 2 views) and third (sport, 3 views), all readable, first and third
     * updatable by any caller.
     */
    private function seedReadPosts(Database $database): void
    {
        $this->deleteReadPosts($database);

        $database->createCollection(Collection::create(
            id: self::READ_POSTS,
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
        ] as $id => [$category, $views, $updatable]) {
            $database->createDocument(self::READ_POSTS, new Document([
                '$id' => $id,
                '$permissions' => [
                    Permission::read(Role::any()),
                    ...($updatable ? [Permission::update(Role::any())] : []),
                ],
                'category' => $category,
                'views' => $views,
            ]));
        }
    }

    private function deleteReadPosts(Database $database): void
    {
        if ($database->collectionExists(self::READ_POSTS)) {
            $database->deleteCollection(self::READ_POSTS);
        }
    }

    private static function readInteger(mixed $value): ?int
    {
        return \is_numeric($value) ? (int) $value : null;
    }
}
