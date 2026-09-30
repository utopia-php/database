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
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
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
        $this->database->createCollection(new Collection(
            id: 'tasks',
            attributes: [
                Attribute::datetime('due', default: '2026-01-31T12:30:00.000+00:00', filters: ['datetime']),
                Attribute::datetime('reminder', filters: ['datetime']),
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
}
