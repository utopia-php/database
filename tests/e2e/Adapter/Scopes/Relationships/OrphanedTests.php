<?php

namespace Tests\E2E\Adapter\Scopes\Relationships;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Id;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\Role;

trait OrphanedTests
{
    public function testOrphanedRelationshipUpdateDocument(): void
    {
        $database = $this->getDatabase();
        $articles = $this->orphanedArticles($database);
        if ($articles === null) {
            return;
        }

        $updated = $database->updateDocument($articles, 'article1', new Document(['name' => 'updated']));
        $this->assertSame('updated', $updated->getAttribute('name'), 'updateDocument must succeed past relationships to deleted collections');
        $article = $database->getDocument($articles, 'article1');
        $this->assertSame('updated', $article->getAttribute('name'));
        $this->assertSame('author1', $article->getAttribute('author'), 'updateDocument must leave an untouched orphaned relationship as stored');
        $this->assertSame('editor1', $article->getAttribute('editor'));

        $updated = $database->updateDocument($articles, 'article1', $article->setAttribute('name', 'whole'));
        $this->assertSame('whole', $updated->getAttribute('name'), 'updateDocument must take back the whole document it read');
        $this->assertSame('author1', $database->getDocument($articles, 'article1')->getAttribute('author'));

        $updated = $database->updateDocument($articles, 'article1', new Document([
            'name' => 'linked',
            'comments' => ['comment2'],
            'tags' => ['tag2'],
            'topics' => ['topic2'],
        ]));
        $this->assertSame('linked', $updated->getAttribute('name'), 'updateDocument must drop a write to an orphaned relationship that stores nothing');

        $updated = $database->updateDocument($articles, 'article1', new Document(['author' => 'author2']));
        $this->assertSame('author2', $updated->getAttribute('author'), 'A write to an orphaned relationship stores the raw ID');
        $this->assertSame('author2', $database->getDocument($articles, 'article1')->getAttribute('author'));

        $updated = $database->updateDocument($articles, 'article1', new Document(['author' => null]));
        $this->assertNull($updated->getAttribute('author'));
        $this->assertNull($database->getDocument($articles, 'article1')->getAttribute('author'));
    }

    public function testOrphanedRelationshipCreateDocument(): void
    {
        $database = $this->getDatabase();
        $articles = $this->orphanedArticles($database);
        if ($articles === null) {
            return;
        }

        $created = $database->createDocument($articles, new Document([
            '$id' => 'article3',
            '$permissions' => self::orphanedPermissions(),
            'name' => 'third',
        ]));
        $this->assertSame('third', $created->getAttribute('name'), 'createDocument must succeed past relationships to deleted collections');

        $created = $database->createDocument($articles, new Document([
            '$id' => 'article4',
            '$permissions' => self::orphanedPermissions(),
            'name' => 'fourth',
            'author' => 'author1',
            'comments' => ['comment2'],
            'tags' => ['tag2'],
            'topics' => ['topic2'],
        ]));
        $this->assertSame('author1', $created->getAttribute('author'), 'createDocument stores the raw ID of an orphaned relationship');
        $this->assertSame('author1', $database->getDocument($articles, 'article4')->getAttribute('author'));
    }

    public function testOrphanedRelationshipGetDocument(): void
    {
        $database = $this->getDatabase();
        $articles = $this->orphanedArticles($database);
        if ($articles === null) {
            return;
        }

        $article = $database->getDocument($articles, 'article1');
        $this->assertSame('first', $article->getAttribute('name'), 'getDocument must read a document whose relationships point at deleted collections');
        $this->assertSame('author1', $article->getAttribute('author'), 'An orphaned relationship keeps its stored value unpopulated');
        $this->assertSame('editor1', $article->getAttribute('editor'), 'An orphaned relationship keeps its stored value unpopulated');

        $article = $database->getDocument($articles, 'article1', [Query::select(['name', 'author.*'])]);
        $this->assertSame('first', $article->getAttribute('name'), 'getDocument selecting an orphaned relationship must read the document');
        $this->assertSame('author1', $article->getAttribute('author'));
    }

    public function testOrphanedRelationshipFind(): void
    {
        $database = $this->getDatabase();
        $articles = $this->orphanedArticles($database);
        if ($articles === null) {
            return;
        }

        $ids = static fn (array $documents): array => \array_map(static fn (mixed $document): string => $document instanceof Document ? $document->getId() : '', $documents);

        $found = $database->find($articles, [Query::orderAsc('$id')]);
        $this->assertSame(['article1', 'article2'], $ids($found), 'find must list documents whose relationships point at deleted collections');
        $this->assertSame('author1', $found[0]->getAttribute('author'));

        $found = $database->find($articles, [Query::select(['name']), Query::orderAsc('$id')]);
        $this->assertSame(['article1', 'article2'], $ids($found), 'find with a select must list documents whose relationships point at deleted collections');
        $this->assertSame('first', $found[0]->getAttribute('name'));

        $found = $database->find($articles, [Query::select(['name', 'author.*']), Query::orderAsc('$id')]);
        $this->assertSame(['article1', 'article2'], $ids($found), 'find selecting an orphaned relationship must list its documents');
        $this->assertSame('author1', $found[0]->getAttribute('author'), 'A selected orphaned relationship returns its stored value');

        $found = $database->find($articles, [Query::select(['*', 'author.name', 'comments.name', 'tags.name', 'topics.name']), Query::orderAsc('$id')]);
        $this->assertSame(['article1', 'article2'], $ids($found), 'find selecting through orphaned relationships must list their documents');
        $this->assertSame('author1', $found[0]->getAttribute('author'));

        $found = $database->find($articles, [Query::equal('author', ['author1']), Query::orderAsc('$id')]);
        $this->assertSame(['article1', 'article2'], $ids($found), 'find must filter on the stored value of an orphaned relationship');
    }

    public function testOrphanedRelationshipDeleteDocument(): void
    {
        $database = $this->getDatabase();
        $articles = $this->orphanedArticles($database);
        if ($articles === null) {
            return;
        }

        $this->assertTrue($database->deleteDocument($articles, 'article1'), 'deleteDocument must ignore onDelete actions of relationships to deleted collections');
        $this->assertTrue($database->getDocument($articles, 'article1')->isEmpty());

        $this->assertSame(1, $database->deleteDocuments($articles), 'deleteDocuments must ignore onDelete actions of relationships to deleted collections');
        $this->assertSame([], $database->find($articles));
    }

    public function testOrphanedRelationshipUpsertDocument(): void
    {
        $database = $this->getDatabase();
        $articles = $this->orphanedArticles($database);
        if ($articles === null) {
            return;
        }

        if (! $database->getAdapter()->hasFeature(Feature\Upserts::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $upserted = $database->upsertDocument($articles, new Document([
            '$id' => 'article2',
            '$permissions' => self::orphanedPermissions(),
            'name' => 'upserted',
            'author' => 'author1',
            'editor' => 'editor1',
        ]));
        $this->assertSame('upserted', $upserted->getAttribute('name'), 'upsertDocument must update a document whose relationships point at deleted collections');
        $this->assertSame('upserted', $database->getDocument($articles, 'article2')->getAttribute('name'));

        $upserted = $database->upsertDocument($articles, new Document([
            '$id' => 'article5',
            '$permissions' => self::orphanedPermissions(),
            'name' => 'fifth',
            'author' => 'author2',
        ]));
        $this->assertSame('fifth', $upserted->getAttribute('name'), 'upsertDocument must create a document whose relationships point at deleted collections');
        $this->assertSame('author2', $database->getDocument($articles, 'article5')->getAttribute('author'));
    }

    public function testOrphanedRelationshipUpdateDocuments(): void
    {
        $database = $this->getDatabase();
        $articles = $this->orphanedArticles($database);
        if ($articles === null) {
            return;
        }

        $count = $database->updateDocuments($articles, new Document(['name' => 'bulk']));
        $this->assertSame(2, $count, 'updateDocuments must update documents whose relationships point at deleted collections');
        foreach ($database->find($articles) as $document) {
            $this->assertSame('bulk', $document->getAttribute('name'));
            $this->assertSame('author1', $document->getAttribute('author'));
        }
    }

    /**
     * A collection of articles relating to collections since deleted, as 7.x could leave them: one-way many-to-one
     * relationships it holds (onDelete setNull and restrict), the child sides of one-way many-to-one relationships
     * other collections held on it, a one-to-many and a many-to-many. Null when the adapter has no relationships.
     */
    private function orphanedArticles(Database $database): ?string
    {
        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return null;
        }

        $suffix = Id::unique();
        $articles = 'orphanArticles'.$suffix;
        $authors = 'orphanAuthors'.$suffix;
        $editors = 'orphanEditors'.$suffix;
        $comments = 'orphanComments'.$suffix;
        $likes = 'orphanLikes'.$suffix;
        $tags = 'orphanTags'.$suffix;
        $topics = 'orphanTopics'.$suffix;
        $permissions = self::orphanedPermissions();

        foreach ([$articles, $authors, $editors, $comments, $likes, $tags, $topics] as $collection) {
            $database->createCollection(Collection::create(
                id: $collection,
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: $permissions,
            ));
        }

        $database->createRelationship($articles, Relationship::manyToOne(relatedCollection: $authors, key: 'author', twoWayKey: 'articles', onDelete: RelationshipDeleteAction::SetNull));
        $database->createRelationship($articles, Relationship::manyToOne(relatedCollection: $editors, key: 'editor', twoWayKey: 'articles', onDelete: RelationshipDeleteAction::Restrict));
        $database->createRelationship($comments, Relationship::manyToOne(relatedCollection: $articles, key: 'article', twoWayKey: 'comments', onDelete: RelationshipDeleteAction::SetNull));
        $database->createRelationship($likes, Relationship::manyToOne(relatedCollection: $articles, key: 'article', twoWayKey: 'likes', onDelete: RelationshipDeleteAction::Restrict));
        $database->createRelationship($articles, Relationship::oneToMany(relatedCollection: $tags, key: 'tags', twoWayKey: 'article', onDelete: RelationshipDeleteAction::Cascade));
        $database->createRelationship($articles, Relationship::manyToMany(relatedCollection: $topics, key: 'topics', twoWayKey: 'articles', onDelete: RelationshipDeleteAction::Restrict));

        $database->createDocument($authors, new Document(['$id' => 'author1', '$permissions' => $permissions, 'name' => 'author']));
        $database->createDocument($editors, new Document(['$id' => 'editor1', '$permissions' => $permissions, 'name' => 'editor']));
        $database->createDocument($topics, new Document(['$id' => 'topic1', '$permissions' => $permissions, 'name' => 'topic']));
        foreach (['article1' => 'first', 'article2' => 'second'] as $id => $name) {
            $database->createDocument($articles, new Document([
                '$id' => $id,
                '$permissions' => $permissions,
                'name' => $name,
                'author' => 'author1',
                'editor' => 'editor1',
            ]));
        }
        $database->createDocument($comments, new Document(['$id' => 'comment1', '$permissions' => $permissions, 'name' => 'comment', 'article' => 'article1']));
        $database->createDocument($likes, new Document(['$id' => 'like1', '$permissions' => $permissions, 'name' => 'like', 'article' => 'article1']));
        $database->createDocument($tags, new Document(['$id' => 'tag1', '$permissions' => $permissions, 'name' => 'tag', 'article' => 'article1']));
        $database->updateDocument($articles, 'article1', new Document(['topics' => ['topic1']]));

        foreach ([$authors, $editors, $comments, $likes, $tags, $topics] as $collection) {
            $database->getAdapter()->deleteCollection($collection);
            $database->getAuthorization()->skip(fn (): bool => $database->silent(fn (): bool => $database->deleteDocument(Database::METADATA, $collection)));
            $database->purgeCachedCollection($collection);
        }

        return $articles;
    }

    /**
     * @return list<string>
     */
    private static function orphanedPermissions(): array
    {
        return [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
    }
}
