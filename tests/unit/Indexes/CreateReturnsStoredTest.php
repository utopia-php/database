<?php

namespace Tests\Unit\Indexes;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Index;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\OrderDirection;

final class CreateReturnsStoredTest extends TestCase
{
    public function testCreateIndexReturnsTheStoredModel(): void
    {
        $database = $this->database();

        $created = $database->createIndex('books', Index::key('by_title', ['title'], [64], [OrderDirection::Desc]));

        $this->assertSame([null], $created->lengths);
        $this->assertSame([OrderDirection::Desc], $created->orders);
        $this->assertSame($created->toDocument()->getArrayCopy(), $this->stored($database, 'by_title')->toDocument()->getArrayCopy());
    }

    public function testCreateIndexRefusesAKeyThatDiffersOnlyInCase(): void
    {
        $database = $this->database();
        $database->createIndex('books', Index::key('by_title', ['title']));

        $this->expectException(DuplicateException::class);

        $database->createIndex('books', Index::key('BY_TITLE', ['title']));
    }

    public function testCreateIndexRefusesAnIndexWithoutAttributes(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Missing attributes');

        $this->database()->createIndex('books', Index::key('empty', []));
    }

    public function testRenameIndexStoresTheIndexUnderTheNewKey(): void
    {
        $database = $this->database();
        $database->createIndex('books', Index::key('by_title', ['title']));

        $database->renameIndex('books', 'by_title', 'title_lookup');

        $this->assertSame(['title_lookup'], $this->keys($database));
        $this->assertSame(['title'], $this->stored($database, 'title_lookup')->attributes);
    }

    public function testRenameIndexRefusesAKeyInUse(): void
    {
        $database = $this->database();
        $database->createIndex('books', Index::key('by_title', ['title']));
        $database->createIndex('books', Index::unique('by_isbn', ['isbn']));

        $this->expectException(DuplicateException::class);

        $database->renameIndex('books', 'by_title', 'by_isbn');
    }

    public function testDeleteIndexRemovesItFromTheMetadata(): void
    {
        $database = $this->database();
        $database->createIndex('books', Index::key('by_title', ['title']));
        $database->createIndex('books', Index::unique('by_isbn', ['isbn']));

        $database->deleteIndex('books', 'by_title');

        $this->assertSame(['by_isbn'], $this->keys($database));
    }

    public function testDeleteIndexOfAMissingKeyIsNotFound(): void
    {
        $this->expectException(NotFoundException::class);

        $this->database()->deleteIndex('books', 'missing');
    }

    private function database(): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('index_returns_stored')
            ->setNamespace('index_returns_stored_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create('books', attributes: [
            Attribute::string('title', 64),
            Attribute::string('isbn', 32),
        ]));

        return $database;
    }

    /**
     * @return list<string>
     */
    private function keys(Database $database): array
    {
        return \array_map(static fn (Index $index): string => $index->key, $database->getCollection('books')->indexes());
    }

    private function stored(Database $database, string $key): Index
    {
        foreach ($database->getCollection('books')->indexes() as $index) {
            if ($index->key === $key) {
                return $index;
            }
        }

        $this->fail('Index '.$key.' is missing from the collection metadata');
    }
}
