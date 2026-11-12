<?php

namespace Tests\Unit\Hook;

use PDO;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Operator;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class QueriesWritesTest extends TestCase
{
    public function testAnUpsertOfARowTheHookHidesStillNeedsUpdatePermission(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $database->setAuthorization($authorization)->setDatabase('queries_writes')->setNamespace('queries_writes_'.\uniqid());
        $database->addHook(new Permissions());
        $database->create();
        $database->createCollection(Collection::create(
            id: 'notes',
            attributes: [Attribute::string(key: 'title', size: 64), Attribute::string(key: 'status', size: 16)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: true,
        ));
        $authorization->skip(static fn (): Document => $database->createDocument('notes', new Document([
            '$id' => 'n1',
            'title' => 'owner text',
            'status' => 'archived',
            '$permissions' => [Permission::update(Role::user('owner'))],
        ])));
        $recorder = new QueriesRecorder(QueriesRecorder::adding(Query::notEqual('status', ['archived'])));
        $database->addHook($recorder);

        try {
            $database->upsertDocument('notes', new Document(['$id' => 'n1', 'title' => 'overwritten', 'status' => 'open']));
            $this->fail('An upsert overwrote a row its caller may not update');
        } catch (AuthorizationException) {
        }

        $this->assertSame([], $recorder->calls);
        $database->removeHook($recorder);
        $this->assertSame('owner text', $authorization->skip(static fn (): Document => $database->getDocument('notes', 'n1'))->getAttribute('title'));
    }

    public function testUpdateDocumentsGivesOnNextTheValueAnOperatorStoredWhileTheHookHidesTheRow(): void
    {
        [$database, $recorder] = $this->hidingPosts();
        $views = [];

        $modified = $database->updateDocuments(
            HookFixture::COLLECTION,
            new Document(['views' => Operator::increment(5)]),
            [Query::equal('title', ['first'])],
            onNext: static function (Document $document) use (&$views): void {
                $views[] = $document->getAttribute('views');
            },
        );

        $this->assertSame(1, $modified);
        $this->assertSame([6], $views);
        $this->assertSame([], $recorder->calls);
    }

    public function testUpdateDocumentReturnsTheValueAnOperatorStoredWhileTheHookHidesTheRow(): void
    {
        [$database, $recorder] = $this->hidingPosts();

        $updated = $database->updateDocument(HookFixture::COLLECTION, 'first', new Document(['views' => Operator::increment(5)]));

        $this->assertSame(6, $updated->getAttribute('views'));
        $this->assertSame([], $recorder->calls);
    }

    public function testACallerReadInsideAWriteCallbackStillRunsTheHook(): void
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['first', 'second']);
        $recorder = new QueriesRecorder(QueriesRecorder::adding(Query::equal('title', ['second'])));
        $database->addHook($recorder);
        $counts = [];

        $database->updateDocuments(
            HookFixture::COLLECTION,
            new Document(['views' => 9]),
            [Query::equal('title', ['first'])],
            onNext: static function () use ($database, &$counts): void {
                $counts[] = $database->count(HookFixture::COLLECTION);
            },
        );

        $this->assertSame([1], $counts);
        $this->assertSame([[Event::DocumentCount, HookFixture::COLLECTION]], $recorder->calls);
    }

    public function testARestrictedDeleteIsRefusedWhileTheHookHidesTheRelatedRow(): void
    {
        $database = $this->albums(RelationshipDeleteAction::Restrict);
        $database->updateDocument('artists', 'a1', new Document(['albums' => ['b1']]));
        $recorder = new QueriesRecorder(QueriesRecorder::adding(Query::notEqual('status', ['archived'])));
        $database->addHook($recorder);

        try {
            $database->deleteDocument('artists', 'a1');
            $this->fail('An artist with a related album was deleted');
        } catch (RestrictedException $error) {
            $this->assertSame('Cannot delete document because it has at least one related document.', $error->getMessage());
        }

        $this->assertSame([], $recorder->calls);
        $database->removeHook($recorder);
        $this->assertFalse($database->getDocument('artists', 'a1')->isEmpty());
    }

    public function testAnUpdateUnlinksARelatedRowTheHookHides(): void
    {
        $database = $this->albums(RelationshipDeleteAction::SetNull);
        $recorder = new QueriesRecorder(QueriesRecorder::adding(Query::notEqual('status', ['archived'])));
        $database->addHook($recorder);

        $updated = $database->updateDocument('artists', 'a1', new Document(['albums' => ['b2']]));

        $this->assertSame(['b2'], $this->ids($updated->getAttribute('albums')));
        $this->assertSame([], $recorder->calls);
        $database->removeHook($recorder);
        $this->assertNull($database->getDocument('albums', 'b1')->getAttribute('artist'));
    }

    public function testWritesOfRelatedDocumentsDoNotRunTheHook(): void
    {
        $database = $this->albums(RelationshipDeleteAction::Cascade);
        $recorder = new QueriesRecorder();
        $database->addHook($recorder);

        $created = $database->createDocument('artists', new Document([
            '$id' => 'a2',
            'status' => 'open',
            'albums' => [new Document(['$id' => 'b3', 'status' => 'open'])],
        ]));
        $database->updateDocument('albums', 'b3', new Document(['status' => 'archived']));
        $deleted = $database->deleteDocument('artists', 'a2');

        $this->assertSame(['b3'], $this->ids($created->getAttribute('albums')));
        $this->assertTrue($deleted);
        $this->assertSame([], $recorder->calls);
    }

    public function testASnapshotTakenWhileQueryHooksAreSkippedSkipsThem(): void
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['first', 'second']);
        $recorder = new QueriesRecorder(QueriesRecorder::adding(Query::equal('title', ['first'])));
        $database->addHook($recorder);

        $skipped = $database->skipQueryHooks(static fn () => $database->snapshot());

        $this->assertSame(2, $database->withSnapshot($skipped, static fn (): int => $database->count(HookFixture::COLLECTION)));
        $this->assertSame([], $recorder->calls);
        $this->assertSame(1, $database->withSnapshot($database->snapshot(), static fn (): int => $database->count(HookFixture::COLLECTION)));
        $this->assertSame([[Event::DocumentCount, HookFixture::COLLECTION]], $recorder->calls);
    }

    /**
     * @return array{Database, QueriesRecorder}
     */
    private function hidingPosts(): array
    {
        $database = HookFixture::sqlite();
        HookFixture::seed($database, ['first', 'second']);
        $recorder = new QueriesRecorder(QueriesRecorder::adding(Query::equal('title', ['nothing'])));
        $database->addHook($recorder);

        return [$database, $recorder];
    }

    /**
     * Artist a1 with album b1, which is archived, and album b2.
     */
    private function albums(RelationshipDeleteAction $onDelete): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database = new Database(new Memory(), new Cache(new None()));
        $database->setAuthorization($authorization)->setDatabase('queries_writes')->setNamespace('queries_writes_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships());
        $database->addHook(new Permissions());

        $permissions = [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
        foreach (['artists', 'albums'] as $collection) {
            $database->createCollection(Collection::create(
                id: $collection,
                attributes: [Attribute::string(key: 'status', size: 16)],
                permissions: $permissions,
                documentSecurity: false,
            ));
        }
        $database->createRelationship('artists', Relationship::oneToMany('albums', key: 'albums', twoWay: true, twoWayKey: 'artist', onDelete: $onDelete));
        $database->createDocument('albums', new Document(['$id' => 'b1', 'status' => 'archived']));
        $database->createDocument('albums', new Document(['$id' => 'b2', 'status' => 'open']));
        $database->createDocument('artists', new Document(['$id' => 'a1', 'status' => 'open', 'albums' => ['b1', 'b2']]));

        return $database;
    }

    /**
     * @return list<string>
     */
    private function ids(mixed $documents): array
    {
        $this->assertIsArray($documents);
        $this->assertContainsOnlyInstancesOf(Document::class, $documents);

        return \array_values(\array_map(static fn (Document $document): string => $document->getId(), $documents));
    }
}
