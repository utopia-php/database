<?php

namespace Tests\Unit\PermissionScope;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * A joined collection is visible exactly as listing it directly would be, and
 * adding a join never changes which main-collection rows the caller can read.
 */
final class JoinVisibilityTest extends TestCase
{
    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $this->database
            ->setDatabase('join_visibility')
            ->setNamespace('join_visibility_'.\uniqid())
            ->setAuthorization(new Authorization());
        $this->database->addHook(new Permissions());
        $this->database->create();

        $this->database->createCollection(new Collection(
            id: 'customers',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::integer(key: 'visits'),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: true,
        ));
        $this->database->createCollection(new Collection(
            id: 'profiles',
            attributes: [
                Attribute::string(key: 'customerId', size: 64),
                Attribute::string(key: 'bio', size: 64),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: true,
        ));
        $this->database->createCollection(new Collection(
            id: 'orders',
            attributes: [
                Attribute::string(key: 'customerId', size: 64),
                Attribute::integer(key: 'amount'),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: true,
        ));
        $this->database->createCollection(new Collection(
            id: 'notes',
            attributes: [
                Attribute::string(key: 'customerId', size: 64),
                Attribute::string(key: 'text', size: 64),
            ],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));
        $this->database->createCollection(new Collection(
            id: 'ledger',
            attributes: [
                Attribute::string(key: 'customerId', size: 64),
                Attribute::integer(key: 'balance'),
            ],
            permissions: [Permission::create(Role::any())],
            documentSecurity: false,
        ));

        $this->create('customers', 'open', ['name' => 'Open', 'visits' => 1], [Permission::read(Role::any())]);
        $this->create('customers', 'bare', ['name' => 'Bare', 'visits' => 10], []);
        $this->create('profiles', 'open-profile', ['customerId' => 'open', 'bio' => 'Hello'], [Permission::read(Role::any())]);
        $this->create('orders', 'public-order', ['customerId' => 'open', 'amount' => 100], [Permission::read(Role::any())]);
        $this->create('orders', 'secret-order', ['customerId' => 'open', 'amount' => 9999], [Permission::read(Role::user('other'))]);
        $this->create('notes', 'alice-note', ['customerId' => 'open', 'text' => 'mine'], [Permission::read(Role::user('alice'))]);
        $this->create('notes', 'bob-note', ['customerId' => 'open', 'text' => 'theirs'], [Permission::read(Role::user('bob'))]);
        $this->create('ledger', 'entry', ['customerId' => 'open', 'balance' => 5], [Permission::read(Role::any())]);

        $authorization = $this->database->getAuthorization();
        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());
        $authorization->addRole(Role::user('alice')->toString());
    }

    public function testJoinKeepsMainRowsReadableThroughTheCollectionGrant(): void
    {
        $join = Query::leftJoin('profiles', '$id', 'customerId', '=', 'profile');

        $this->assertSame(['bare', 'open'], $this->ids($this->database->find('customers')));
        $this->assertSame(
            ['bare', 'open'],
            $this->ids($this->database->find('customers', [$join, Query::select(['name', 'profile.bio'])])),
            'A left join is additive: it must not hide a row the collection grant makes readable',
        );

        $this->assertSame(2, $this->database->count('customers'));
        $this->assertSame(2, $this->database->count('customers', [$join]));

        $this->assertSame(11, $this->database->sum('customers', 'visits'));
        $this->assertSame(11, $this->database->sum('customers', 'visits', [$join]));

        $this->assertSame('bare', $this->database->getDocument('customers', 'bare')->getId());
        $this->assertSame('bare', $this->database->getDocument('customers', 'bare', [$join])->getId());
    }

    public function testJoinedCollectionWithCollectionGrantShowsEveryRow(): void
    {
        $join = Query::join('orders', '$id', 'customerId', '=', 'ord');

        $this->assertSame([100, 9999], $this->integers($this->database->find('orders'), 'amount'));
        $this->assertSame(
            [100, 9999],
            $this->integers($this->database->find('customers', [$join, Query::select(['name', 'ord.amount'])]), 'ord.amount'),
            'The collection grant makes every order readable directly, so the join must show every order',
        );

        $this->assertSame(2, $this->database->count('orders'));
        $this->assertSame(2, $this->database->count('customers', [$join]));
        $this->assertSame(2, $this->database->sum('customers', 'visits', [$join]));
    }

    public function testJoinedCollectionWithOnlyDocumentGrantsShowsTheCallerRows(): void
    {
        $join = Query::join('notes', '$id', 'customerId', '=', 'note');

        $this->assertSame(['mine'], $this->strings($this->database->find('notes'), 'text'));
        $this->assertSame(
            ['mine'],
            $this->strings($this->database->find('customers', [$join, Query::select(['name', 'note.text'])]), 'note.text'),
            'Without a collection grant the joined rows are filtered per document, exactly like a direct list',
        );

        $this->assertSame(1, $this->database->count('notes'));
        $this->assertSame(1, $this->database->count('customers', [$join]));
        $this->assertSame(1, $this->database->sum('customers', 'visits', [$join]));

        $document = $this->database->getDocument('customers', 'open', [$join, Query::select(['name', 'note.text'])]);
        $this->assertSame('mine', $document->getAttribute('note.text'));
    }

    public function testTheAdapterHonoursTheGrantOnlyOnJoinReads(): void
    {
        $adapter = $this->database->getAdapter();
        $collection = clone $this->database->getCollection('customers');
        $collection->setAttribute(Database::COLLECTION_GRANTED, true);
        $collection->setAttribute(Database::JOIN_DOCUMENT_SECURITY, ['profiles' => false]);

        $this->assertSame(['open'], $this->ids($adapter->find($collection)), 'Without joins the Database layer grants by disabling authorization, never by marking the collection');
        $this->assertSame(1, $adapter->count($collection));
        $this->assertSame(1, $adapter->sum($collection, 'visits'));
        $this->assertSame(['bare', 'open'], $this->ids($adapter->find($collection, [Query::leftJoin('profiles', '$id', 'customerId', '=', 'profile')])));
    }

    public function testJoinedCollectionWithoutGrantOrDocumentSecurityIsRejected(): void
    {
        $join = Query::join('ledger', '$id', 'customerId', '=', 'ledger');

        $this->assertRejected(fn () => $this->database->find('ledger'));
        $this->assertRejected(fn () => $this->database->find('customers', [$join]), "joined collection 'ledger'");
        $this->assertRejected(fn () => $this->database->count('customers', [$join]), "joined collection 'ledger'");
        $this->assertRejected(fn () => $this->database->sum('customers', 'visits', [$join]), "joined collection 'ledger'");
        $this->assertRejected(fn () => $this->database->getDocument('customers', 'open', [$join]), "joined collection 'ledger'");
    }

    /**
     * @param  array<string, mixed>  $attributes
     * @param  array<string>  $permissions
     */
    private function create(string $collection, string $id, array $attributes, array $permissions): void
    {
        $this->database->createDocument($collection, new Document([
            '$id' => $id,
            '$permissions' => $permissions,
            ...$attributes,
        ]));
    }

    private function assertRejected(callable $read, string $message = ''): void
    {
        try {
            $read();
        } catch (AuthorizationException $exception) {
            $this->assertStringContainsString($message, $exception->getMessage());

            return;
        }

        $this->fail('Reading a collection the caller holds neither a collection nor a document grant on must be rejected');
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function ids(array $documents): array
    {
        $ids = \array_values(\array_unique(\array_map(static fn (Document $document): string => $document->getId(), $documents)));
        \sort($ids);

        return $ids;
    }

    /**
     * @param  array<Document>  $documents
     * @return list<int>
     */
    private function integers(array $documents, string $attribute): array
    {
        $values = [];
        foreach ($documents as $document) {
            $value = $document->getAttribute($attribute);
            if (\is_numeric($value)) {
                $values[] = (int) $value;
            }
        }
        \sort($values);

        return $values;
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function strings(array $documents, string $attribute): array
    {
        $values = [];
        foreach ($documents as $document) {
            $value = $document->getAttribute($attribute);
            if (\is_string($value)) {
                $values[] = $value;
            }
        }
        \sort($values);

        return $values;
    }
}
