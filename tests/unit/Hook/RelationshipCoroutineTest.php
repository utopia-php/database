<?php

namespace Tests\Unit\Hook;

use Closure;
use PDO;
use PHPUnit\Framework\TestCase;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipType;
use Utopia\Database\Validator\Authorization;

use function Swoole\Coroutine\run;

final class RelationshipCoroutineTest extends TestCase
{
    private const string FILTER = 'pausing';

    private const string PAUSE = 'pause';

    private ?Channel $paused = null;

    private ?Channel $resumed = null;

    private bool $armed = false;

    protected function setUp(): void
    {
        if (! \extension_loaded('swoole')) {
            $this->markTestSkipped('ext-swoole is required for coroutines sharing a handle');
        }
    }

    public function testANestedWriteInOneCoroutineLeavesAnotherCoroutinesNestedWritesWhole(): void
    {
        $seen = [];

        $this->inCoroutine(function () use (&$seen): void {
            $database = $this->database();
            foreach (['levelOne', 'levelTwo', 'levelThree', 'shelves', 'books'] as $collection) {
                $database->createCollection($this->collection($collection));
            }
            $this->relate($database, 'levelOne', 'levelTwo', RelationshipType::OneToMany, 'children', 'parent');
            $this->relate($database, 'levelTwo', 'levelThree', RelationshipType::OneToMany, 'children', 'parent');
            $this->relate($database, 'shelves', 'books', RelationshipType::OneToMany, 'books', 'shelf');
            $this->armed = true;

            $done = new Channel(1);
            Coroutine::create(function () use ($database, $done): void {
                $database->createDocument('levelOne', new Document([
                    '$id' => 'one',
                    'name' => 'one',
                    'children' => [new Document([
                        '$id' => 'two',
                        'name' => 'two',
                        'children' => [new Document(['$id' => 'three', 'name' => self::PAUSE])],
                    ])],
                ]));
                $done->push(true);
            });

            $this->assertTrue($this->pausedChannel()->pop(5), 'The first coroutine never paused');
            $database->createDocument('shelves', new Document([
                '$id' => 'fiction',
                'name' => 'fiction',
                'books' => [new Document(['$id' => 'dune', 'name' => 'dune'])],
            ]));
            $this->resumedChannel()->push(true);
            $done->pop();

            $seen['book'] = $database->getDocument('books', 'dune')->getId();
            $books = $database->getDocument('shelves', 'fiction')->getAttribute('books', []);
            $this->assertIsArray($books);
            $seen['shelf'] = \array_map(static function (mixed $book): string {
                self::assertInstanceOf(Document::class, $book);

                return $book->getId();
            }, $books);
            $seen['three'] = $database->getDocument('levelThree', 'three')->getId();
        });

        $this->assertSame(['book' => 'dune', 'shelf' => ['dune'], 'three' => 'three'], $seen);
    }

    public function testACascadeInOneCoroutineLeavesAnotherCoroutinesCascadeWhole(): void
    {
        $seen = [];

        $this->inCoroutine(function () use (&$seen): void {
            $database = $this->database();
            foreach (['people', 'pets', 'passports'] as $collection) {
                $database->createCollection($this->collection($collection));
            }
            $this->relate($database, 'people', 'pets', RelationshipType::OneToMany, 'pets', 'owner');
            $this->relate($database, 'people', 'passports', RelationshipType::OneToOne, 'passport', 'person', RelationshipDeleteAction::Cascade);

            $database->createDocument('people', new Document([
                '$id' => 'paused',
                'name' => 'paused',
                'pets' => [new Document(['$id' => 'rex', 'name' => self::PAUSE])],
                'passport' => new Document(['$id' => 'pausedPassport', 'name' => 'pausedPassport']),
            ]));
            $database->createDocument('people', new Document([
                '$id' => 'other',
                'name' => 'other',
                'passport' => new Document(['$id' => 'otherPassport', 'name' => 'otherPassport']),
            ]));
            $this->armed = true;

            $done = new Channel(1);
            Coroutine::create(function () use ($database, $done): void {
                $database->deleteDocument('passports', 'pausedPassport');
                $done->push(true);
            });

            $this->assertTrue($this->pausedChannel()->pop(5), 'The first coroutine never paused');
            $database->deleteDocument('people', 'other');
            $this->resumedChannel()->push(true);
            $done->pop();

            $seen['otherPassportDeleted'] = $database->getDocument('passports', 'otherPassport')->isEmpty();
            $seen['pausedDeleted'] = $database->getDocument('people', 'paused')->isEmpty();
            $seen['rexOwner'] = $database->getDocument('pets', 'rex')->getAttribute('owner');
        });

        $this->assertSame(['otherPassportDeleted' => true, 'pausedDeleted' => true, 'rexOwner' => null], $seen);
    }

    private function database(): Database
    {
        $this->paused = new Channel(1);
        $this->resumed = new Channel(1);
        $this->armed = false;
        $pause = function (mixed $value): mixed {
            if ($value === self::PAUSE && $this->armed) {
                $this->armed = false;
                $this->pausedChannel()->push(true);
                $this->resumedChannel()->pop();
            }

            return $value;
        };

        $database = new Database(
            new SQLite(new PDO('sqlite::memory:')),
            new Cache(new None()),
            [self::FILTER => ['encode' => $pause, 'decode' => static fn (mixed $value): mixed => $value]],
        );
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('relationships')
            ->setNamespace('relationships_'.\uniqid());
        $database->addHook(new Relationships());
        $database->create();

        return $database;
    }

    private function collection(string $id): Collection
    {
        return Collection::create(
            id: $id,
            attributes: [Attribute::string(key: 'name', size: 64, filters: [self::FILTER])],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            documentSecurity: false,
        );
    }

    private function relate(
        Database $database,
        string $collection,
        string $relatedCollection,
        RelationshipType $type,
        string $key,
        string $twoWayKey,
        RelationshipDeleteAction $onDelete = RelationshipDeleteAction::SetNull,
    ): void {
        $database->createRelationship($collection, Relationship::fromArray([
            'relatedCollection' => $relatedCollection,
            'relationType' => $type,
            'twoWay' => true,
            'key' => $key,
            'twoWayKey' => $twoWayKey,
            'onDelete' => $onDelete,
        ]));
    }

    private function pausedChannel(): Channel
    {
        return $this->paused ?? throw new \LogicException('The database is not built');
    }

    private function resumedChannel(): Channel
    {
        return $this->resumed ?? throw new \LogicException('The database is not built');
    }

    private function inCoroutine(Closure $test): void
    {
        $hookFlags = Runtime::getHookFlags();

        try {
            run($test);
        } finally {
            Runtime::setHookFlags($hookFlags);
        }
    }
}
