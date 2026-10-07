<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Operator;
use Utopia\Database\Validator\Authorization;

final class UpsertNewDocumentOperatorTest extends TestCase
{
    private const string NAMESPACE = 'upsert_operators';

    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $this->database->create();
        $this->database->createCollection(Collection::create(
            id: 'tasks',
            attributes: [
                Attribute::datetime('due', default: '2026-01-31T12:30:00.000+00:00'),
                Attribute::datetime('reminder'),
                Attribute::boolean('active', default: true),
                Attribute::boolean('archived'),
                Attribute::bigInteger('counter', default: PHP_INT_MAX - 5),
                Attribute::bigInteger('floor', default: PHP_INT_MIN + 5),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
    }

    /**
     * @return iterable<string, array{string, Operator, mixed}>
     */
    public static function operators(): iterable
    {
        yield 'days added to the default' => ['due', Operator::dateAddDays(1), '2026-02-01T12:30:00.000+00:00'];
        yield 'days subtracted from the default' => ['due', Operator::dateSubDays(31), '2025-12-31T12:30:00.000+00:00'];
        yield 'no days added' => ['due', Operator::dateAddDays(0), '2026-01-31T12:30:00.000+00:00'];
        yield 'days added without a default' => ['reminder', Operator::dateAddDays(3), null];
        yield 'days subtracted without a default' => ['reminder', Operator::dateSubDays(3), null];
        yield 'default toggled off' => ['active', Operator::toggle(), false];
        yield 'missing default toggled on' => ['archived', Operator::toggle(), true];
        yield 'increment within the limit' => ['counter', Operator::increment(5, PHP_INT_MAX), PHP_INT_MAX];
        yield 'increment past the limit keeps the default' => ['counter', Operator::increment(10, PHP_INT_MAX), PHP_INT_MAX - 5];
        yield 'multiply past the limit keeps the default' => ['counter', Operator::multiply(2, PHP_INT_MAX), PHP_INT_MAX - 5];
        yield 'decrement past the limit keeps the default' => ['floor', Operator::decrement(10, PHP_INT_MIN), PHP_INT_MIN + 5];
    }

    #[DataProvider('operators')]
    public function testAnOperatorOnANewDocumentAppliesToTheDefault(string $attribute, Operator $operator, mixed $expected): void
    {
        $this->database->upsertDocument('tasks', new Document(['$id' => 'created', $attribute => $operator]));

        $this->assertSame($expected, $this->database->getDocument('tasks', 'created')->getAttribute($attribute));
    }

    public function testDaysAddedToANewDocumentMatchTheDaysAddedToAnExistingOne(): void
    {
        $this->database->createDocument('tasks', new Document(['$id' => 'existing']));

        $this->database->upsertDocuments('tasks', [
            new Document(['$id' => 'existing', 'due' => Operator::dateAddDays(2)]),
            new Document(['$id' => 'created', 'due' => Operator::dateAddDays(2)]),
        ]);

        $this->assertSame('2026-02-02T12:30:00.000+00:00', $this->database->getDocument('tasks', 'existing')->getAttribute('due'));
        $this->assertSame('2026-02-02T12:30:00.000+00:00', $this->database->getDocument('tasks', 'created')->getAttribute('due'));
    }

    public function testAnOperatorOnlyALaterDocumentOfTheBatchCarriesIsApplied(): void
    {
        $this->database->upsertDocuments('tasks', [
            new Document(['$id' => 'plain', 'archived' => true]),
            new Document(['$id' => 'toggled', 'active' => Operator::toggle()]),
        ]);

        $this->assertTrue($this->database->getDocument('tasks', 'plain')->getAttribute('active'));
        $this->assertTrue($this->database->getDocument('tasks', 'plain')->getAttribute('archived'));
        $this->assertFalse($this->database->getDocument('tasks', 'toggled')->getAttribute('active'));
    }

    public function testAnEmptyBatchWritesNothing(): void
    {
        $adapter = $this->database->getAdapter();
        $this->assertInstanceOf(SQLite::class, $adapter);
        $collection = $this->database->getCollection('tasks');

        $this->assertSame([], $adapter->createDocuments($collection, []));
        $this->assertSame([], $adapter->upsertDocuments($collection, []));
        $this->assertSame([], $this->database->find('tasks'));
    }

    public function testAnUpsertThatBreaksAUniqueIndexIsAUniqueViolation(): void
    {
        $this->database->createCollection(Collection::create(
            id: 'accounts',
            attributes: [Attribute::string('email', size: 64)],
            indexes: [Index::unique(key: 'unique_email', attributes: ['email'])],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
        $this->database->createDocument('accounts', new Document(['$id' => 'first', 'email' => 'shared@example.com']));

        try {
            $this->database->upsertDocument('accounts', new Document(['$id' => 'second', 'email' => 'shared@example.com']));
            $this->fail('an upsert breaking a unique index must be refused');
        } catch (UniqueException $error) {
            $this->assertInstanceOf(\PDOException::class, $error->getPrevious());
        }

        $this->assertTrue($this->database->getDocument('accounts', 'second')->isEmpty());
    }

    public function testNowSetOnANewDocumentIsTheTimeOfTheWrite(): void
    {
        $before = new \DateTimeImmutable('-1 second');
        $this->database->upsertDocument('tasks', new Document(['$id' => 'created', 'reminder' => Operator::dateSetNow()]));
        $after = new \DateTimeImmutable('+1 second');

        $reminder = $this->database->getDocument('tasks', 'created')->getAttribute('reminder');
        $this->assertIsString($reminder);
        $written = new \DateTimeImmutable($reminder);
        $this->assertGreaterThanOrEqual($before, $written);
        $this->assertLessThanOrEqual($after, $written);
    }
}
