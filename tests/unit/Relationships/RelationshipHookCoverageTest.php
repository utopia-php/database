<?php

namespace Tests\Unit\Relationships;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Operator;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\Validator\Authorization;

final class RelationshipHookCoverageTest extends TestCase
{
    /**
     * @return array<string, array{Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'memory' => [static fn (): Adapter => new Memory()],
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
        ];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testANestedPathThroughAPlainAttributeMatchesNothing(Closure $adapter): void
    {
        $database = $this->library($adapter());

        $this->assertSame(['notes'], $this->ids($database->find('books', [Query::equal('author.publisher.name', ['Acme'])])));
        $this->assertSame([], $database->find('books', [Query::equal('author.name.first', ['Ada'])]));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testANestedPathWhoseHopFindsNoParentMatchesNothing(Closure $adapter): void
    {
        $database = $this->library($adapter());
        $database->createDocument('publishers', new Document([Document::ID => 'lonely', 'name' => 'Lonely']));

        $this->assertSame([], $database->find('books', [Query::equal('author.publisher.name', ['Lonely'])]));
        $this->assertSame([], $database->find('books', [Query::equal('author.publisher.name', ['Nobody'])]));

        $database->createCollection(Collection::create(id: 'countries', attributes: [Attribute::string(key: 'name', size: 64)], permissions: $this->permissions()));
        $database->createRelationship('publishers', Relationship::manyToOne(relatedCollection: 'countries', twoWay: true, key: 'country', twoWayKey: 'publishers'));
        $database->createDocument('countries', new Document([Document::ID => 'nowhere', 'name' => 'Nowhere']));
        $database->createDocument('countries', new Document([Document::ID => 'home', 'name' => 'Home']));
        $database->updateDocument('publishers', 'acme', new Document(['country' => 'home']));

        $this->assertSame(['notes'], $this->ids($database->find('books', [Query::equal('author.publisher.country.name', ['Home'])])));
        $this->assertSame([], $database->find('books', [Query::equal('author.publisher.country.name', ['Nowhere'])]), 'a hop that finds nothing ends the path before the next hop');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRemovingAValueThatIsNotAnIdentifierKeepsTheLinks(Closure $adapter): void
    {
        $database = $this->library($adapter());
        $database->createDocument('books', new Document([Document::ID => 'essays', 'title' => 'Essays', 'author' => 'ada']));

        $database->skipValidation(fn (): Document => $database->updateDocument('authors', 'ada', new Document([
            'books' => Operator::arrayRemove(5),
        ])));

        $this->assertEqualsCanonicalizing(['essays', 'notes'], $this->ids($database->getDocument('authors', 'ada')->getAttribute('books')));
        $author = $database->getDocument('books', 'essays')->getAttribute('author');
        $this->assertInstanceOf(Document::class, $author);
        $this->assertSame('ada', $author->getId());
    }

    /**
     * @return array<string, array{string}>
     */
    public static function races(): array
    {
        return [
            'the child was deleted meanwhile' => ['gone'],
            'the child was linked meanwhile' => ['linked'],
        ];
    }

    #[DataProvider('races')]
    public function testALinkWhoseChildChangedSinceTheBulkLinkIsSkipped(string $race): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database = new class (new SQLite(new PDO('sqlite::memory:')), new Cache(new None()), $race) extends Database {
            public bool $racing = false;

            public function __construct(Adapter $adapter, Cache $cache, private readonly string $race)
            {
                parent::__construct($adapter, $cache);
            }

            public function getDocument(string $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                $document = parent::getDocument($collection, $id, $queries, $forUpdate);
                if (! $this->racing || ! $forUpdate || $collection !== 'child') {
                    return $document;
                }

                return $this->race === 'gone' ? new Document() : $document->setAttribute('parent', 'parent1');
            }
        };
        $database
            ->setAuthorization($authorization)
            ->setDatabase('relationship_hook_coverage')
            ->setNamespace('race_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships($database));
        $database->createCollection(Collection::create(id: 'parent', permissions: $this->permissions(), documentSecurity: false));
        $database->createCollection(Collection::create(id: 'child', permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: true));
        $database->createRelationship('parent', Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull));
        $database->createDocument('parent', new Document([Document::ID => 'parent1']));
        $database->createDocument('child', new Document([Document::ID => 'child1', Document::PERMISSIONS => [Permission::read(Role::any())]]));

        $database->racing = true;
        $database->updateDocument('parent', 'parent1', new Document(['children' => ['child1']]));
        $database->racing = false;

        $child = $database->skipRelationships(fn (): Document => $database->getDocument('child', 'child1'));
        $this->assertNull($child->getAttribute('parent'), 'a child that changed since the bulk link is left as it is');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testARelationshipChangeNestedPastTheMaximumDepthIsDropped(Closure $adapter): void
    {
        $database = $this->library($adapter());
        $database->createCollection(Collection::create(id: 'countries', attributes: [Attribute::string(key: 'name', size: 64)], permissions: $this->permissions()));
        $database->createRelationship('publishers', Relationship::manyToOne(relatedCollection: 'countries', twoWay: true, key: 'country', twoWayKey: 'publishers'));
        $database->createDocument('countries', new Document([Document::ID => 'home', 'name' => 'Home']));
        $database->createDocument('countries', new Document([Document::ID => 'away', 'name' => 'Away']));
        $database->updateDocument('publishers', 'acme', new Document(['country' => 'home']));

        $database->updateDocument('books', 'notes', new Document([
            'author' => new Document([
                Document::ID => 'ada',
                'name' => 'Ada Lovelace',
                'publisher' => new Document([
                    Document::ID => 'acme',
                    'name' => 'Acme Press',
                    'country' => new Document([Document::ID => 'away', 'name' => 'Abroad']),
                ]),
            ]),
        ]));

        $this->assertSame('Ada Lovelace', $database->getDocument('authors', 'ada')->getAttribute('name'));
        $this->assertSame('Acme Press', $database->getDocument('publishers', 'acme')->getAttribute('name'));
        $this->assertSame(['acme'], $this->ids($database->find('publishers', [Query::equal('country', ['home'])])), 'the link three levels down is past the maximum depth and stays');
        $this->assertSame('Away', $database->getDocument('countries', 'away')->getAttribute('name'), 'so is the nested document, which is not written');
    }

    /**
     * @return list<string>
     */
    private function ids(mixed $documents): array
    {
        $this->assertIsArray($documents);
        $ids = [];
        foreach ($documents as $document) {
            $this->assertInstanceOf(Document::class, $document);
            $ids[] = $document->getId();
        }

        return $ids;
    }

    private function library(Adapter $adapter): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('relationship_hook_coverage')
            ->setNamespace('library_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships($database));

        foreach (['books' => 'title', 'authors' => 'name', 'publishers' => 'name'] as $collection => $attribute) {
            $database->createCollection(Collection::create(id: $collection, attributes: [Attribute::string(key: $attribute, size: 64)], permissions: $this->permissions()));
        }
        $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));
        $database->createRelationship('authors', Relationship::manyToOne(relatedCollection: 'publishers', twoWay: true, key: 'publisher', twoWayKey: 'authors'));

        $database->createDocument('publishers', new Document([Document::ID => 'acme', 'name' => 'Acme']));
        $database->createDocument('authors', new Document([Document::ID => 'ada', 'name' => 'Ada', 'publisher' => 'acme']));
        $database->createDocument('books', new Document([Document::ID => 'notes', 'title' => 'Notes', 'author' => 'ada']));

        return $database;
    }

    /**
     * @return list<string>
     */
    private function permissions(): array
    {
        return [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
    }
}
