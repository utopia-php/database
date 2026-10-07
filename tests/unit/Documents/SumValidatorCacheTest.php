<?php

namespace Tests\Unit\Documents;

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

final class SumValidatorCacheTest extends TestCase
{
    private const string COLLECTION = 'books';

    public function testRepeatedSumsAreValidatedAlike(): void
    {
        $database = $this->database();

        for ($i = 0; $i < 3; $i++) {
            $this->assertSame(7, $database->sum(self::COLLECTION, 'pages'));
            $this->assertSame(
                'Invalid query: Aggregate sum requires a numeric attribute that is not an array: title',
                $this->failure(fn () => $database->sum(self::COLLECTION, 'title')),
            );
        }
    }

    public function testASchemaChangeSelectsAnotherValidator(): void
    {
        $database = $this->database();
        $this->assertSame(7, $database->sum(self::COLLECTION, 'pages'));

        $database->deleteAttribute(self::COLLECTION, 'pages');
        $this->assertNotNull($this->failure(fn () => $database->sum(self::COLLECTION, 'pages')));

        $database->createAttribute(self::COLLECTION, Attribute::string(key: 'pages', size: 8));
        $this->assertSame(
            'Invalid query: Aggregate sum requires a numeric attribute that is not an array: pages',
            $this->failure(fn () => $database->sum(self::COLLECTION, 'pages')),
        );
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

    private function database(): Database
    {
        $database = new Database(new Memory(), new Cache(new MemoryCache()));
        $database->setDatabase('sums')->setNamespace('sums_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64), Attribute::integer(key: 'pages')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'dune', 'title' => 'Dune', 'pages' => 7]));

        return $database;
    }
}
