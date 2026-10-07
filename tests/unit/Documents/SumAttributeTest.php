<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Relationship;

final class SumAttributeTest extends TestCase
{
    private const string COLLECTION = 'books';

    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new Memory(), new Cache(new MemoryCache()));
        $this->database->setDatabase('sums')->setNamespace('sums_'.\uniqid());
        $this->database->create();
        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $this->database->createCollection(Collection::create(id: 'authors', permissions: $permissions));
        $this->database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'title', size: 64),
                Attribute::integer(key: 'pages'),
                Attribute::float(key: 'rating'),
                Attribute::integer(key: 'chapters', array: true),
            ],
            permissions: $permissions,
        ));
        $this->database->createRelationship(self::COLLECTION, Relationship::manyToOne(relatedCollection: 'authors', key: 'author', twoWayKey: 'books'));
        $this->database->createDocument(self::COLLECTION, new Document([Document::ID => 'dune', 'title' => 'Dune', 'pages' => 7, 'rating' => 4.5, 'chapters' => [1, 2]]));
        $this->database->createDocument(self::COLLECTION, new Document([Document::ID => 'emma', 'title' => 'Emma', 'pages' => 5, 'rating' => 3.0, 'chapters' => [3]]));
    }

    public function testADeclaredIntegerIsSummed(): void
    {
        $this->assertSame(12, $this->database->sum(self::COLLECTION, 'pages'));
    }

    public function testADeclaredFloatIsSummed(): void
    {
        $this->assertSame(7.5, $this->database->sum(self::COLLECTION, 'rating'));
    }

    /**
     * @return array<string, array{string, string}>
     */
    public static function refusedAttributes(): array
    {
        return [
            'string' => ['title', 'Invalid query: Aggregate sum requires a numeric attribute that is not an array: title'],
            'numeric array' => ['chapters', 'Invalid query: Aggregate sum requires a numeric attribute that is not an array: chapters'],
            'virtual relationship' => ['books', 'Invalid query: Attribute not found in schema: books'],
            'relationship' => ['author', 'Invalid query: Aggregate sum requires a numeric attribute that is not an array: author'],
            'undeclared' => ['missing', 'Invalid query: Attribute not found in schema: missing'],
        ];
    }

    #[DataProvider('refusedAttributes')]
    public function testAnAttributeThatIsNotASingleNumberIsRefused(string $attribute, string $message): void
    {
        $this->assertSame($message, $this->failure(fn () => $this->database->sum(self::COLLECTION, $attribute)));
    }

    private function failure(callable $sum): ?string
    {
        try {
            $sum();
        } catch (Throwable $error) {
            return $error->getMessage();
        }

        return null;
    }
}
