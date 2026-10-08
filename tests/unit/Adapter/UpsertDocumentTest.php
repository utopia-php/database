<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Change;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class UpsertDocumentTest extends TestCase
{
    private const string COLLECTION = 'counters';

    private SQLite $adapter;

    private Database $database;

    #[\Override]
    protected function setUp(): void
    {
        $this->adapter = new SQLite(new PDO('sqlite::memory:'));
        $this->database = new Database($this->adapter, new Cache(new None()));
        $this->database
            ->setAuthorization(new Authorization())
            ->setDatabase('upserts')
            ->setNamespace('upserts_'.\uniqid());
        $this->database->getAuthorization()->addRole(Role::any()->toString());
        $this->database->create();
        $this->database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'title', size: 64),
                Attribute::integer(key: 'views'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: false,
        ));
    }

    public function testUpsertDocumentInsertsADocumentNothingStoredYet(): void
    {
        $written = $this->adapter->upsertDocument($this->collection(), new Change(new Document(), $this->document('one', 'first', 1)));

        $this->assertSame('one', $written->getId());
        $this->assertSame(['first', 1], $this->stored('one'));
    }

    public function testUpsertDocumentReplacesTheStoredDocument(): void
    {
        $this->adapter->upsertDocument($this->collection(), new Change(new Document(), $this->document('one', 'first', 1)));
        $stored = $this->database->getDocument(self::COLLECTION, 'one');

        $written = $this->adapter->upsertDocument($this->collection(), new Change($stored, $this->document('one', 'second', 5)));

        $this->assertSame('second', $written->getAttribute('title'));
        $this->assertSame(['second', 5], $this->stored('one'));
        $this->assertSame(1, $this->database->count(self::COLLECTION));
    }

    public function testUpsertDocumentsWritesEveryChangeInOrder(): void
    {
        $written = $this->adapter->upsertDocuments($this->collection(), [
            new Change(new Document(), $this->document('one', 'first', 1)),
            new Change(new Document(), $this->document('two', 'second', 2)),
        ]);

        $this->assertSame(['one', 'two'], \array_map(static fn (Document $document): string => $document->getId(), $written));
        $this->assertSame(['first', 1], $this->stored('one'));
        $this->assertSame(['second', 2], $this->stored('two'));
        $this->assertSame([], $this->adapter->upsertDocuments($this->collection(), []));
    }

    public function testAnIncreaseAddsTheValueToTheStoredOne(): void
    {
        $this->database->upsertDocuments(self::COLLECTION, [$this->document('one', 'first', 2)], increase: 'views');
        $this->database->upsertDocuments(self::COLLECTION, [$this->document('one', 'first', 3)], increase: 'views');

        $this->assertSame(['first', 5], $this->stored('one'));
    }

    public function testWithoutAnIncreaseTheValueIsReplaced(): void
    {
        $this->database->upsertDocuments(self::COLLECTION, [$this->document('one', 'first', 2)]);
        $this->database->upsertDocuments(self::COLLECTION, [$this->document('one', 'first', 3)]);

        $this->assertSame(['first', 3], $this->stored('one'));
    }

    public function testASingleUpsertThroughTheDatabaseReturnsTheWrittenDocument(): void
    {
        $created = $this->database->upsertDocument(self::COLLECTION, $this->document('one', 'first', 1));
        $updated = $this->database->upsertDocument(self::COLLECTION, $this->document('one', 'second', 2));

        $this->assertSame(['first', 1], [$created->getAttribute('title'), $created->getAttribute('views')]);
        $this->assertSame(['second', 2], [$updated->getAttribute('title'), $updated->getAttribute('views')]);
        $this->assertSame(['second', 2], $this->stored('one'));
    }

    public function testAPoolUpsertsOnItsConnection(): void
    {
        $pool = $this->pool($this->adapter);

        $written = $pool->upsertDocument($this->collection(), new Change(new Document(), $this->document('one', 'pooled', 4)));

        $this->assertSame('one', $written->getId());
        $this->assertSame(['pooled', 4], $this->stored('one'));
    }

    public function testAPoolOverAnAdapterWithoutUpsertsRefusesThem(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support upserts');

        $this->pool(new Memory())->upsertDocument($this->collection(), new Change(new Document(), $this->document('one', 'first', 1)));
    }

    private function collection(): Collection
    {
        return $this->database->getCollection(self::COLLECTION);
    }

    private function document(string $id, string $title, int $views): Document
    {
        $now = DateTime::now();

        return new Document([
            Document::ID => $id,
            Document::PERMISSIONS => [],
            Document::CREATED_AT => $now,
            Document::UPDATED_AT => $now,
            'title' => $title,
            'views' => $views,
        ]);
    }

    /**
     * @return array{mixed, mixed}
     */
    private function stored(string $id): array
    {
        $document = $this->database->getDocument(self::COLLECTION, $id);

        return [$document->getAttribute('title'), $document->getAttribute('views')];
    }

    private function pool(Adapter $adapter): Pool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(static fn (callable $callback): mixed => $callback($adapter));

        $pool = new Pool($connections);
        $pool->setAuthorization($this->database->getAuthorization());
        $pool->setDatabase($this->database->getDatabase());
        $pool->setNamespace($this->database->getNamespace());

        return $pool;
    }
}
