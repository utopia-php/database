<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

final class SQLJoinedReadsTest extends TestCase
{
    public function testALockingReadWithAJoinIsRefused(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        $join = Query::join('notes', 'note', [Query::on('$id', 'customerId')]);

        $this->assertSame('c1', $database->getDocument('customers', 'c1', [$join])->getId());

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Cannot lock a document for update when join queries are present');

        $database->withTransaction(fn (): Document => $database->getDocument('customers', 'c1', [$join], forUpdate: true));
    }

    public function testAFullOuterJoinFollowedByANestedLeftJoinReachesBothHalves(): void
    {
        $queries = [
            Query::fullOuterJoin('notes', 'note', [Query::on('$id', 'customerId')]),
            Query::leftJoin('replies', 'reply', [Query::on('note.$id', 'noteId')]),
            Query::select(['name', 'note.body', 'reply.text']),
        ];

        $expected = [
            '["c1","n1","r1"]',
            '["c1","n2",null]',
            '["c2","n3",null]',
            '["c3",null,null]',
            '[null,"n4","r2"]',
        ];
        $this->assertSame($expected, $this->rows($this->emulated()->find('customers', $queries), ['name', 'note.body', 'reply.text']));
        $this->assertSame($expected, $this->rows($this->native()->find('customers', $queries), ['name', 'note.body', 'reply.text']));
    }

    public function testAFullOuterJoinFollowedByANestedRightJoinKeepsOnlyMatchedReplies(): void
    {
        $queries = [
            Query::fullOuterJoin('notes', 'note', [Query::on('$id', 'customerId')]),
            Query::rightJoin('replies', 'reply', [Query::on('note.$id', 'noteId')]),
            Query::select(['name', 'note.body', 'reply.text']),
        ];

        $expected = ['["c1","n1","r1"]', '[null,"n4","r2"]', '[null,null,"r3"]'];
        $this->assertSame($expected, $this->rows($this->emulated()->find('customers', $queries), ['name', 'note.body', 'reply.text']));
        $this->assertSame($expected, $this->rows($this->native()->find('customers', $queries), ['name', 'note.body', 'reply.text']));
    }

    public function testADistinctFullOuterJoinWithoutNamedSelectsIsOrdered(): void
    {
        $join = Query::fullOuterJoin('notes', 'note', [Query::on('$id', 'customerId')]);

        foreach ([[], [Query::select(['*'])]] as $select) {
            $queries = [Query::distinct(), $join, ...$select, Query::orderDesc('name')];
            $emulated = $this->ordered($this->emulated()->find('customers', $queries));

            $this->assertSame(['c3', 'c2', 'c1', 'c1', null], $emulated);
            $this->assertSame($this->ordered($this->native()->find('customers', $queries)), $emulated);
        }
    }

    private function emulated(): Database
    {
        return $this->database(new SQLite(new PDO('sqlite::memory:')));
    }

    private function native(): Database
    {
        return $this->database(new NativeFullOuterJoinSQLite(new PDO('sqlite::memory:')));
    }

    private function database(SQLite $adapter): Database
    {
        $database = new Database($adapter, new Cache(new NoCache()));
        $database->setDatabase('joined_reads')->setNamespace('joined_reads')->setAuthorization(new Authorization());
        $database->create();

        $collections = [
            'customers' => [Attribute::string('name', size: 16)],
            'notes' => [Attribute::string('customerId', size: 16), Attribute::string('body', size: 16)],
            'replies' => [Attribute::string('noteId', size: 16), Attribute::string('text', size: 16)],
        ];
        foreach ($collections as $id => $attributes) {
            $database->createCollection(Collection::create(
                id: $id,
                attributes: $attributes,
                permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
                documentSecurity: false,
            ));
        }

        foreach (['c1', 'c2', 'c3'] as $customer) {
            $database->createDocument('customers', new Document(['$id' => $customer, 'name' => $customer]));
        }
        foreach (['n1' => 'c1', 'n2' => 'c1', 'n3' => 'c2', 'n4' => 'cx'] as $note => $customer) {
            $database->createDocument('notes', new Document(['$id' => $note, 'customerId' => $customer, 'body' => $note]));
        }
        foreach (['r1' => 'n1', 'r2' => 'n4', 'r3' => 'nx'] as $reply => $note) {
            $database->createDocument('replies', new Document(['$id' => $reply, 'noteId' => $note, 'text' => $reply]));
        }

        return $database;
    }

    /**
     * @param array<Document> $documents
     * @param list<string> $attributes
     * @return list<string>
     */
    private function rows(array $documents, array $attributes): array
    {
        $rows = [];
        foreach ($documents as $document) {
            $rows[] = \json_encode(\array_map(static fn (string $attribute): mixed => $document->getAttribute($attribute), $attributes), JSON_THROW_ON_ERROR);
        }
        \sort($rows);

        return $rows;
    }

    /**
     * @param array<Document> $documents
     * @return list<mixed>
     */
    private function ordered(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): mixed => $document->getAttribute('name'), $documents));
    }
}
