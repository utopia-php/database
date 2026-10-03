<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

final class GetDocumentQueryValidationTest extends TestCase
{
    private const string COLLECTION = 'notes';

    public function testAReadWithoutQueriesReturnsTheWholeDocument(): void
    {
        $database = $this->database();

        $document = $database->getDocument(self::COLLECTION, 'note');
        $cached = $database->getDocument(self::COLLECTION, 'note');

        $this->assertSame('ada', $document->getAttribute('author'));
        $this->assertSame('hello', $document->getAttribute('body'));
        $this->assertSame($document->getArrayCopy(), $cached->getArrayCopy());
    }

    public function testASelectOfAnUnknownAttributeIsRejected(): void
    {
        $database = $this->database();

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid query: Attribute not found in schema: missing');

        $database->getDocument(self::COLLECTION, 'note', [Query::select(['missing'])]);
    }

    public function testAFilterIsRejected(): void
    {
        $database = $this->database();

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid query method: equal');

        $database->getDocument(self::COLLECTION, 'note', [Query::equal('author', ['ada'])]);
    }

    public function testASelectOfAKnownAttributeIsApplied(): void
    {
        $database = $this->database();

        $document = $database->getDocument(self::COLLECTION, 'note', [Query::select(['author'])]);

        $this->assertSame('ada', $document->getAttribute('author'));
        $this->assertFalse($document->offsetExists('body'));
    }

    private function database(): Database
    {
        $database = new Database(new Memory(), new Cache(new MemoryCache()));
        $database->setDatabase('validation')->setNamespace('validation_'.\uniqid());
        $database->create();
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'author', size: 32), Attribute::string(key: 'body', size: 256)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'note', 'author' => 'ada', 'body' => 'hello']));

        return $database;
    }
}
