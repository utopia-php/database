<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

final class SQLiteVectorQueryTest extends TestCase
{
    private const string NAMESPACE = 'vector_query';

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
            id: 'items',
            attributes: [Attribute::string('name', size: 16)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));
        foreach (['first', 'second', 'third'] as $name) {
            $this->database->createDocument('items', new Document(['$id' => $name, 'name' => $name]));
        }
    }

    public function testAVectorQueryIsRefusedWhereTheEngineHasNoVectors(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid query: Attribute not found in schema: embedding');

        $this->database->find('items', [Query::vectorCosine('embedding', [1.0, 0.0, 0.0])]);
    }

    public function testWithoutValidationAVectorQueryAddsNoDistanceOrderOrFilter(): void
    {
        $this->database->disableValidation();

        $items = $this->database->find('items', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::orderDesc('name'),
        ]);

        $this->assertSame(['third', 'second', 'first'], \array_map(static fn (Document $item): string => $item->getId(), $items));
    }
}
