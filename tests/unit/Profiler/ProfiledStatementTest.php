<?php

namespace Tests\Unit\Profiler;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Profiler\QueryLog;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * Each read the profiler logs carries the values bound to its statement, the collection it reads and
 * the operation that ran it.
 */
final class ProfiledStatementTest extends TestCase
{
    private const string NAMESPACE = 'profiled_statement';

    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $this->database
            ->setDatabase('profiled_statement')
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $this->database->addHook(new Permissions());
        $this->database->create();
        $this->database->createCollection(Collection::create(
            id: 'items',
            attributes: [
                Attribute::string(key: 'category', size: 16),
                Attribute::integer(key: 'price'),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        foreach ([['i1', 'first', 10], ['i2', 'second', 20]] as [$id, $category, $price]) {
            $this->database->createDocument('items', new Document([
                '$id' => $id,
                '$permissions' => [Permission::read(Role::any())],
                'category' => $category,
                'price' => $price,
            ]));
        }
    }

    public function testAFilteredFindLogsItsValuesAndCollection(): void
    {
        $log = $this->logOn(fn (): int => \count($this->database->find('items', [Query::equal('category', ['second'])])));

        $this->assertContains('second', $log->bindings);
        $this->assertSame('items', $log->collection);
        $this->assertSame(Event::DocumentFind->value, $log->operation);
    }

    public function testCountAndSumLogTheirValuesAndCollection(): void
    {
        $count = $this->logOn(fn (): int => $this->database->count('items', [Query::greaterThan('price', 15)]));
        $this->assertContains(15, $count->bindings);
        $this->assertSame('items', $count->collection);
        $this->assertSame(Event::DocumentCount->value, $count->operation);

        $sum = $this->logOn(fn (): int|float => $this->database->sum('items', 'price', [Query::equal('category', ['first'])], 5));
        $this->assertContains('first', $sum->bindings);
        $this->assertContains(5, $sum->bindings);
        $this->assertSame('items', $sum->collection);
        $this->assertSame(Event::DocumentSum->value, $sum->operation);
    }

    public function testAnUnfilteredReadLogsItsCollection(): void
    {
        $count = $this->database->getAuthorization()->skip(fn (): QueryLog => $this->logOn(fn (): int => $this->database->count('items')));
        $this->assertSame([], $count->bindings);
        $this->assertSame('items', $count->collection);
        $this->assertSame(Event::DocumentCount->value, $count->operation);

        $read = $this->logOn(fn (): string => $this->database->getDocument('items', 'i2')->getId());
        $this->assertSame([':_uid' => 'i2'], $read->bindings);
        $this->assertSame('items', $read->collection);
        $this->assertSame(Event::DocumentRead->value, $read->operation);
    }

    public function testStatementsRunWhileTheProfilerIsOffAreNotDescribed(): void
    {
        $this->database->find('items', [Query::equal('category', ['first'])]);

        $log = $this->logOn(fn (): int => \count($this->database->find('items', [Query::equal('category', ['second'])])));

        $this->assertNotContains('first', $log->bindings);
        $this->assertContains('second', $log->bindings);
    }

    /**
     * The one statement $read ran on the collection's table.
     *
     * @param  callable(): mixed  $read
     */
    private function logOn(callable $read): QueryLog
    {
        $profiler = $this->database->enableProfiling()->getProfiler();
        $this->assertNotNull($profiler);

        try {
            $profiler->reset();
            $read();
        } finally {
            $this->database->disableProfiling();
        }

        $logs = \array_values(\array_filter(
            $profiler->getLogs(),
            static fn (QueryLog $log): bool => \str_contains($log->query, self::NAMESPACE.'_items`'),
        ));
        $this->assertCount(1, $logs);

        return $logs[0];
    }
}
