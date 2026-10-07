<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

/**
 * cursor() is the one generator: it reads pages of $batchSize (CURSOR_BATCH_SIZE by default), a limit() caps what
 * it yields, an offset or cursorAfter positions the first page, and $forPermission picks the permission it reads
 * under.
 */
final class CursorTest extends TestCase
{
    public function testTheDefaultPageIsTheCursorBatchSize(): void
    {
        /** @var list<int|null> $limits */
        $limits = [];
        $database = $this->pagingDatabase(static function (?int $limit) use (&$limits): void {
            $limits[] = $limit;
        });
        HookFixture::seed($database, \array_map(static fn (int $index): string => 'doc'.$index, \range(1, 150)));

        $this->assertCount(150, \iterator_to_array($database->cursor(HookFixture::COLLECTION), false));
        $this->assertSame(100, Database::CURSOR_BATCH_SIZE);
        $this->assertSame([100, 100], $limits);
    }

    public function testALimitCapsTheDocumentsYielded(): void
    {
        $database = $this->seeded(7);

        $this->assertSame(['doc1', 'doc2', 'doc3'], $this->ids($database->cursor(HookFixture::COLLECTION, [Query::orderAsc('views'), Query::limit(3)], 2)));
    }

    public function testAnOffsetOrACursorPositionsTheFirstPage(): void
    {
        $database = $this->seeded(5);
        $second = $database->getDocument(HookFixture::COLLECTION, 'doc2');

        $this->assertSame(['doc3', 'doc4', 'doc5'], $this->ids($database->cursor(HookFixture::COLLECTION, [Query::orderAsc('views'), Query::offset(2)], 2)));
        $this->assertSame(['doc3', 'doc4', 'doc5'], $this->ids($database->cursor(HookFixture::COLLECTION, [Query::orderAsc('views'), Query::cursorAfter($second)], 2)));
    }

    public function testACursorBeforeIsRefusedWhenTheCursorIsCreated(): void
    {
        $database = $this->seeded(2);
        $first = $database->getDocument(HookFixture::COLLECTION, 'doc1');

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Cursor before not supported in this method.');

        $database->cursor(HookFixture::COLLECTION, [Query::cursorBefore($first)]);
    }

    public function testThePermissionTheCursorReadsUnder(): void
    {
        $database = HookFixture::sqlite();
        $database->addHook(new Permissions());
        $database->createCollection(Collection::create(
            id: 'notes',
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));
        $database->createDocument('notes', new Document([
            Document::ID => 'editable',
            Document::PERMISSIONS => [Permission::read(Role::any()), Permission::update(Role::any())],
            'title' => 'editable',
        ]));
        $database->createDocument('notes', new Document([
            Document::ID => 'readable',
            Document::PERMISSIONS => [Permission::read(Role::any())],
            'title' => 'readable',
        ]));

        $this->assertSame(['editable', 'readable'], $this->ids($database->cursor('notes', [Query::orderAsc('title')])));
        $this->assertSame(['editable'], $this->ids($database->cursor('notes', [Query::orderAsc('title')], forPermission: PermissionType::Update)));
    }

    private function seeded(int $count): Database
    {
        $database = HookFixture::sqlite();
        HookFixture::seed($database, \array_map(static fn (int $index): string => 'doc'.$index, \range(1, $count)));

        return $database;
    }

    /**
     * @param  Closure(?int): void  $record
     */
    private function pagingDatabase(Closure $record): Database
    {
        $database = new class (new Memory(), new Cache(new None()), $record) extends Database {
            public function __construct(Adapter $adapter, Cache $cache, private readonly Closure $record)
            {
                parent::__construct($adapter, $cache);
            }

            #[\Override]
            public function find(string $collection, array $queries = [], PermissionType $forPermission = PermissionType::Read): array
            {
                if ($collection === HookFixture::COLLECTION) {
                    foreach ($queries as $query) {
                        if ($query->getMethod() === Method::Limit) {
                            $limit = $query->getValue();
                            ($this->record)(\is_int($limit) ? $limit : null);
                        }
                    }
                }

                return parent::find($collection, $queries, $forPermission);
            }
        };
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('hooks')
            ->setNamespace('cursor_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: HookFixture::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64), Attribute::integer(key: 'views')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        return $database;
    }

    /**
     * @param  iterable<Document>  $documents
     * @return list<string>
     */
    private function ids(iterable $documents): array
    {
        $ids = [];
        foreach ($documents as $document) {
            $ids[] = $document->getId();
        }

        return $ids;
    }
}
