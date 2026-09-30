<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Query as BaseQuery;

final class SQLSchemaBuilderTest extends TestCase
{
    private const string NAMESPACE = 'schema_builder';

    public function testATableBuiltThroughTheSchemaBuilderIsCreatedFilledReadAndDropped(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $database = new Database(new SQLite($pdo), new Cache(new NoCache()));
        $database->setDatabase(self::NAMESPACE)->setNamespace(self::NAMESPACE)->setAuthorization(new Authorization());
        $database->create();

        $rows = $database->getAuthorization()->skip(function () use ($database): mixed {
            $table = $database->schema()->table(self::NAMESPACE . '_raw_items');
            $table->integer('value');
            $table->string('label', 16);
            $table->create()->execute();

            $database->execute($database->from('raw_items')->set(['value' => 7, 'label' => 'seven'])->insert());
            $database->execute($database->from('raw_items')->set(['value' => 8, 'label' => 'eight'])->insert());

            return $database->execute($database->from('raw_items')->select(['value', 'label'])->filter([BaseQuery::equal('value', [7])]));
        });

        $this->assertIsArray($rows);
        $this->assertCount(1, $rows);
        $this->assertInstanceOf(Document::class, $rows[0]);
        $this->assertSame(['value' => 7, 'label' => 'seven'], $rows[0]->getArrayCopy());

        $database->getAuthorization()->skip(fn (): mixed => $database->schema()->table(self::NAMESPACE . '_raw_items')->drop()->execute());

        $statement = $pdo->query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = '" . self::NAMESPACE . "_raw_items'");
        $this->assertInstanceOf(\PDOStatement::class, $statement);
        $this->assertSame([], $statement->fetchAll(PDO::FETCH_COLUMN));
    }

    public function testTheSchemaBuilderIsRefusedWithoutAQueryBuilder(): void
    {
        $database = new Database(new Memory(), new Cache(new NoCache()));

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Schema builder is not supported by this adapter');

        $database->schema();
    }
}
