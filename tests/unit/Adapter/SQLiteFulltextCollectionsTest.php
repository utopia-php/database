<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

/**
 * A fulltext index is an FTS5 table named after its collection, which the adapter finds again by that name. The name of
 * collection "notes" starts the names of "notes_private"'s tables too, which "notes" must never take for its own: a
 * search would read the other collection's rows, past its permissions, and a delete would drop its tables.
 */
final class SQLiteFulltextCollectionsTest extends TestCase
{
    private const string NAMESPACE = 'fulltext';

    private Database $database;

    #[\Override]
    protected function setUp(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization($authorization);
        $this->database->create();

        foreach (['notes' => Role::any(), 'notes_private' => Role::user('owner')] as $collection => $reader) {
            $this->database->createCollection(Collection::create(
                id: $collection,
                attributes: [Attribute::string('body', size: 128)],
                indexes: [Index::fulltext(key: 'body_search', attributes: ['body'])],
                permissions: [Permission::create(Role::any()), Permission::read($reader)],
                documentSecurity: false,
            ));
        }

        $this->database->createDocument('notes', new Document(['$id' => 'first', 'body' => 'a public note']));
        $this->database->createDocument('notes', new Document(['$id' => 'second', 'body' => 'another public note']));
        $this->database->createDocument('notes_private', new Document(['$id' => 'first', 'body' => 'nothing to see']));
        $this->database->createDocument('notes_private', new Document(['$id' => 'second', 'body' => 'the secret salary']));
    }

    public function testASearchReadsOnlyItsOwnCollectionsIndex(): void
    {
        $this->assertSame(['first', 'second'], $this->search('notes', 'public'));
        $this->assertSame([], $this->search('notes', 'secret'), 'The row of notes_private with the same sequence must not answer for notes');
    }

    public function testDeletingACollectionKeepsTheIndexOfACollectionWhoseIdItStarts(): void
    {
        $this->database->deleteCollection('notes');

        $this->database->createDocument('notes_private', new Document(['$id' => 'third', 'body' => 'another secret']));

        $this->assertSame(['second', 'third'], $this->database->getAuthorization()->skip(fn () => $this->search('notes_private', 'secret')));
    }

    public function testDeletingACollectionKeepsACollectionNamedLikeItsIndex(): void
    {
        $lookalike = 'notes_0123456789abcdef_fts';
        $this->database->createCollection(Collection::create(
            id: $lookalike,
            attributes: [Attribute::string('body', size: 128)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));
        $this->database->createDocument($lookalike, new Document(['$id' => 'kept', 'body' => 'an ordinary row']));

        $this->assertSame(['first', 'second'], $this->search('notes', 'public'));

        $this->database->deleteCollection('notes');

        $this->assertSame(
            ['kept'],
            \array_map(static fn (Document $document): string => $document->getId(), $this->database->find($lookalike)),
            'Only an FTS5 table is an index of notes, never an ordinary table that shares the shape of its name',
        );
    }

    /**
     * @return list<string>
     */
    private function search(string $collection, string $term): array
    {
        return \array_values(\array_map(
            static fn (Document $document): string => $document->getId(),
            $this->database->find($collection, [Query::search('body', $term), Query::orderAsc('$id')]),
        ));
    }
}
