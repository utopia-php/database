<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

/**
 * The Memory adapter matches a regex with PCRE, and refuses a pattern PCRE cannot compile as the engines do instead
 * of reading it as matching nothing.
 */
final class MemoryRegexTest extends TestCase
{
    private const string COLLECTION = 'words';

    private function database(): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setDatabase('regex')
            ->setNamespace('regex')
            ->setAuthorization(new Authorization());
        $database->create();

        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string('name', size: 32)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        foreach (['abc', 'axc'] as $name) {
            $database->createDocument(self::COLLECTION, new Document(['name' => $name]));
        }

        return $database;
    }

    public function testAPatternThatDoesNotCompileIsAQueryError(): void
    {
        $database = $this->database();

        foreach (['((', '[', 'a{2,1}'] as $pattern) {
            try {
                $database->find(self::COLLECTION, [Query::regex('name', $pattern)]);
                $this->fail("The pattern {$pattern} is refused");
            } catch (QueryException $e) {
                $this->assertSame('Invalid regular expression', $e->getMessage());
            }
        }
    }

    public function testAPatternThatCompilesStillMatches(): void
    {
        $this->assertCount(1, $this->database()->find(self::COLLECTION, [Query::regex('name', '^a[b]c$')]));
    }
}
