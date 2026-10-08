<?php

namespace Tests\Unit;

use Closure;
use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Throwable;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Character as CharacterException;
use Utopia\Database\Exception\Contention as ContentionException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception as MongoException;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;

final class EngineErrorMappingTest extends TestCase
{
    private const string NAMESPACE = 'engine';

    private const string DISTINCT_ORDER = 'A distinct() query can only be ordered by a selected attribute on this database';

    /**
     * @return array<string, array{0: Closure(PDOException): Throwable, 1: PDOException, 2: class-string<Throwable>, 3: string}>
     */
    public static function lockConflictProvider(): array
    {
        $deadlock = self::engineError('40001', 1213, 'SQLSTATE[40001]: Serialization failure: 1213 Deadlock found when trying to get lock; try restarting transaction');
        $lockWait = self::engineError('HY000', 1205, 'SQLSTATE[HY000]: General error: 1205 Lock wait timeout exceeded; try restarting transaction');
        $missingTable = self::engineError('42S02', 1146, "SQLSTATE[42S02]: Base table or view not found: 1146 Table 'utopiaTests.engine_orders' doesn't exist");

        return [
            'MariaDB deadlock' => [self::mariaDB(), $deadlock, ContentionException::class, 'Deadlock detected'],
            'MySQL deadlock' => [self::mySQL(), $deadlock, ContentionException::class, 'Deadlock detected'],
            'MariaDB lock wait timeout' => [self::mariaDB(), $lockWait, ContentionException::class, 'Lock wait timeout exceeded'],
            'MySQL lock wait timeout' => [self::mySQL(), $lockWait, ContentionException::class, 'Lock wait timeout exceeded'],
            'MariaDB statement on a missing table' => [self::mariaDB(), $missingTable, NotFoundException::class, 'Collection not found'],
            'MySQL statement on a missing table' => [self::mySQL(), $missingTable, NotFoundException::class, 'Collection not found'],
            'Postgres deadlock' => [
                self::postgres(),
                self::engineError('40P01', 7, "SQLSTATE[40P01]: Deadlock detected: 7 ERROR:  deadlock detected\nDETAIL:  Process 81 waits for ShareLock on transaction 740; blocked by process 82."),
                ContentionException::class,
                'Deadlock detected',
            ],
            'Postgres serialization failure' => [
                self::postgres(),
                self::engineError('40001', 7, 'SQLSTATE[40001]: Serialization failure: 7 ERROR:  could not serialize access due to concurrent update'),
                ContentionException::class,
                'Could not serialize access due to a concurrent update',
            ],
            'Postgres lock not available' => [
                self::postgres(),
                self::engineError('55P03', 7, 'SQLSTATE[55P03]: Lock not available: 7 ERROR:  canceling statement due to lock timeout'),
                ContentionException::class,
                'Lock not available',
            ],
            'SQLite busy database' => [
                self::sqlite(),
                self::engineError('HY000', 5, 'SQLSTATE[HY000]: General error: 5 database is locked'),
                ContentionException::class,
                'Database is locked',
            ],
            'Postgres invalid UTF-8' => [
                self::postgres(),
                self::engineError('22021', 7, 'SQLSTATE[22021]: Character not in repertoire: 7 ERROR:  invalid byte sequence for encoding "UTF8": 0xc3 0x28'),
                CharacterException::class,
                'Invalid character',
            ],
        ];
    }

    /**
     * @param  Closure(PDOException): Throwable  $map
     * @param  class-string<Throwable>  $expected
     */
    #[DataProvider('lockConflictProvider')]
    public function testLockConflictsMissingTablesAndBadCharactersAreMapped(Closure $map, PDOException $error, string $expected, string $message): void
    {
        $this->assertMapped($map, $error, $expected, $message);
    }

    /**
     * @return array<string, array{0: string, 1: class-string<Throwable>, 2: string}>
     */
    public static function undefinedTableProvider(): array
    {
        $line = "\nLINE 1: SELECT \"main\".\"_uid\" FROM \"utopiaTests\".\"engine_orders\" AS \"main\" WHERE \"mian\".\"_uid\" = \$1\n                                                                          ^";
        $hashed = \md5('engine_'.\str_repeat('a', 70));

        return [
            'a statement on a missing table' => ['SQLSTATE[42P01]: Undefined table: 7 ERROR:  relation "utopiaTests.engine_orders" does not exist'.$line, NotFoundException::class, 'Collection not found'],
            'a statement on a missing permissions table' => ['SQLSTATE[42P01]: Undefined table: 7 ERROR:  relation "utopiaTests.engine_orders_perms" does not exist'.$line, NotFoundException::class, 'Collection not found'],
            'a statement on a missing table with a hashed name' => ['SQLSTATE[42P01]: Undefined table: 7 ERROR:  relation "utopiaTests.'.$hashed.'_perms" does not exist'.$line, NotFoundException::class, 'Collection not found'],
            'a DDL statement on a missing table' => ['SQLSTATE[42P01]: Undefined table: 7 ERROR:  relation "utopiaTests.engine_orders" does not exist', NotFoundException::class, 'Collection not found'],
            'a DROP of a missing table' => ['SQLSTATE[42P01]: Undefined table: 7 ERROR:  table "engine_orders" does not exist', NotFoundException::class, 'Collection not found'],
            'a DROP of a missing table with a hashed name' => ['SQLSTATE[42P01]: Undefined table: 7 ERROR:  table "'.$hashed.'" does not exist', NotFoundException::class, 'Collection not found'],
            'a missing table in German' => ["SQLSTATE[42P01]: Undefined table: 7 FEHLER:  Relation \u{BB}utopiaTests.engine_orders\u{AB} existiert nicht".$line, NotFoundException::class, 'Collection not found'],
            'a missing table in French' => ["SQLSTATE[42P01]: Undefined table: 7 ERREUR:  la relation \u{AB}\u{A0}utopiaTests.engine_orders\u{A0}\u{BB} n'existe pas".$line, NotFoundException::class, 'Collection not found'],
            'an undeclared alias' => ['SQLSTATE[42P01]: Undefined table: 7 ERROR:  missing FROM-clause entry for table "mian"'.$line, QueryException::class, 'Query references an undefined table or alias'],
            'an undeclared alias in German' => ["SQLSTATE[42P01]: Undefined table: 7 FEHLER:  fehlender Eintrag in FROM-Klausel f\u{FC}r Tabelle \u{BB}mian\u{AB}".$line, QueryException::class, 'Query references an undefined table or alias'],
            'a table referenced by name instead of its alias' => ['SQLSTATE[42P01]: Undefined table: 7 ERROR:  invalid reference to FROM-clause entry for table "engine_orders"'.$line."\nHINT:  Perhaps you meant to reference the table alias \"main\".", QueryException::class, 'Query references an undefined table or alias'],
            'a relation outside the namespace' => ['SQLSTATE[42P01]: Undefined table: 7 ERROR:  relation "utopiaTests.other_orders" does not exist'.$line, QueryException::class, 'Query references an undefined table or alias'],
            'a message that names nothing' => ['SQLSTATE[42P01]: Undefined table: 7', NotFoundException::class, 'Collection not found'],
        ];
    }

    /**
     * @param  class-string<Throwable>  $expected
     */
    #[DataProvider('undefinedTableProvider')]
    public function testPostgresUndefinedTableIsNotFoundOnlyForACollectionTable(string $message, string $expected, string $mapped): void
    {
        $this->assertMapped(self::postgres(), self::engineError('42P01', 7, $message), $expected, $mapped);
    }

    /**
     * @return array<string, array{0: Closure(PDOException): Throwable}>
     */
    public static function mariaDBFamilyProvider(): array
    {
        return [
            'MariaDB' => [self::mariaDB()],
            'MySQL' => [self::mySQL()],
        ];
    }

    /**
     * @param  Closure(PDOException): Throwable  $map
     */
    #[DataProvider('mariaDBFamilyProvider')]
    public function testIndexOnAColumnTheTableLacksIsAttributeNotFound(Closure $map): void
    {
        $error = self::engineError('42000', 1072, "SQLSTATE[42000]: Syntax error or access violation: 1072 Key column 'name' doesn't exist in table");

        $this->assertMapped($map, $error, NotFoundException::class, 'Attribute not found');
    }

    /**
     * @param  Closure(PDOException): Throwable  $map
     */
    #[DataProvider('mariaDBFamilyProvider')]
    public function testIndexKeyTooLongIsAnIndexError(Closure $map): void
    {
        $error = self::engineError('42000', 1071, 'SQLSTATE[42000]: Syntax error or access violation: 1071 Specified key was too long; max key length is 3072 bytes');

        $this->assertMapped($map, $error, IndexException::class, 'Index key length exceeds the maximum');
    }

    public function testPostgresIndexRowTooLargeIsALimit(): void
    {
        $error = self::engineError('54000', 7, 'SQLSTATE[54000]: Program limit exceeded: 7 ERROR:  index row size 8016 exceeds btree version 4 maximum 2704 for index "engine_orders_by_note"');

        $this->assertMapped(self::postgres(), $error, LimitException::class, 'Index row size exceeds the maximum');
    }

    public function testPostgresProgramLimitOtherThanAnIndexRowStaysRaw(): void
    {
        $error = self::engineError('54000', 7, 'SQLSTATE[54000]: Program limit exceeded: 7 ERROR:  tables can have at most 1600 columns');

        $this->assertSame($error, self::postgres()($error));
    }

    public function testSQLiteUnknownColumnIsAttributeNotFound(): void
    {
        $error = self::engineError('HY000', 1, 'SQLSTATE[HY000]: General error: 1 no such column: items');

        $this->assertMapped(self::sqlite(), $error, NotFoundException::class, 'Attribute not found');
    }

    public function testSQLiteReadOnAnUnknownColumnIsAttributeNotFound(): void
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $database
            ->setDatabase('engine_errors')
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $database->create();
        $database->createCollection(Collection::create(
            id: 'orders',
            attributes: [Attribute::string(key: 'category', size: 20)],
            permissions: [Permission::read(Role::any())],
        ));

        $error = null;
        try {
            $database->skipValidation(fn () => $database->find('orders', [Query::equal('no_such_attribute', ['x'])]));
        } catch (Throwable $caught) {
            $error = $caught;
        }

        $this->assertInstanceOf(NotFoundException::class, $error);
        $this->assertSame('Attribute not found', $error->getMessage());
    }

    /**
     * @return array<string, array{0: string}>
     */
    public static function undefinedFunctionProvider(): array
    {
        return [
            'max over a boolean' => ["SQLSTATE[42883]: Undefined function: 7 ERROR:  function max(boolean) does not exist\nLINE 1: SELECT MAX(\"main\".\"active\") AS \"most\" FROM ...\nHINT:  No function matches the given name and argument types. You might need to add explicit type casts."],
            'min over JSONB' => ['SQLSTATE[42883]: Undefined function: 7 ERROR:  function min(jsonb) does not exist'],
            'an operator on mismatched types' => ['SQLSTATE[42883]: Undefined function: 7 ERROR:  operator does not exist: character varying + integer'],
        ];
    }

    #[DataProvider('undefinedFunctionProvider')]
    public function testPostgresUndefinedFunctionIsAQueryError(string $message): void
    {
        $this->assertMapped(self::postgres(), self::engineError('42883', 7, $message), QueryException::class, 'Query applies a function or operator the attribute type does not support');
    }

    public function testPostgresDistinctReadOrderedByAnUnselectedAttributeIsAQueryErrorInAnyLanguage(): void
    {
        $error = self::engineError('42P10', 7, "SQLSTATE[42P10]: Invalid column reference: 7 FEHLER:  bei SELECT DISTINCT m\u{FC}ssen ORDER-BY-Ausdr\u{FC}cke in der Select-Liste erscheinen\nLINE 1: ...\"main\" ORDER BY \"main\".\"price\" ASC");

        $failure = $this->postgresFindFailure($error, [Query::distinct(), Query::select(['category'])]);

        $this->assertInstanceOf(QueryException::class, $failure);
        $this->assertSame(self::DISTINCT_ORDER, $failure->getMessage());
        $this->assertSame($error, $failure->getPrevious());
    }

    public function testPostgresReadWithoutDistinctLeavesAnUnnamedInvalidColumnReferenceRaw(): void
    {
        $error = self::engineError('42P10', 7, 'SQLSTATE[42P10]: Invalid column reference: 7 FEHLER:  ORDER BY Position 3 ist nicht in der Select-Liste');

        $this->assertSame($error, $this->postgresFindFailure($error, [Query::select(['category'])]));
    }

    public function testPostgresConflictTargetWithoutAConstraintIsNotADistinctError(): void
    {
        $error = self::engineError('42P10', 7, 'SQLSTATE[42P10]: Invalid column reference: 7 ERROR:  there is no unique or exclusion constraint matching the ON CONFLICT specification');

        $this->assertSame($error, self::postgres()($error));
    }

    public function testMongoTypeMismatchIsAnInvalidOperation(): void
    {
        $adapter = new class (new class () extends Client {
            public function __construct()
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }
        }) extends Mongo {
            public function map(Throwable $error): Throwable
            {
                return $this->processException($error);
            }
        };
        $error = new MongoException('Cannot apply $inc to a value of non-numeric type. {_id: ObjectId(\'66f9\')} has the field \'name\' of non-numeric type string', 14);

        $mapped = $adapter->map($error);

        $this->assertInstanceOf(TypeException::class, $mapped);
        $this->assertSame('Invalid operation', $mapped->getMessage());
        $this->assertSame($error, $mapped->getPrevious());
    }

    public function testSQLiteDoesNotTreatTheMySQLTimeoutCodeAsATimeout(): void
    {
        $error = self::engineError('HY000', 3024, 'SQLSTATE[HY000]: General error: 3024 Query execution was interrupted');

        $this->assertSame($error, self::sqlite()($error));
    }

    public function testPostgresDeleteCollectionWithoutItsTableIsNotFoundAndStillDropsThePermissionsTable(): void
    {
        $statements = [];
        $adapter = $this->postgresRecording($statements, self::engineError('42P01', 7, 'SQLSTATE[42P01]: Undefined table: 7 ERROR:  table "engine_orders" does not exist'));

        $error = null;
        try {
            $adapter->deleteCollection('orders');
        } catch (Throwable $caught) {
            $error = $caught;
        }

        $this->assertInstanceOf(NotFoundException::class, $error);
        $this->assertSame('Collection not found', $error->getMessage());
        $this->assertSame([
            'DROP TABLE "utopiaTests"."engine_orders"; DROP TABLE IF EXISTS "utopiaTests"."engine_orders_perms"',
            'DROP TABLE IF EXISTS "utopiaTests"."engine_orders_perms"',
        ], $statements);
    }

    public function testPostgresUniqueIndexOverDuplicateObjectPathValuesIsUnique(): void
    {
        $statements = [];
        $duplicates = self::engineError('23505', 7, "SQLSTATE[23505]: Unique violation: 7 ERROR:  could not create unique index \"engine_orders_unique_country\"\nDETAIL:  Key ((data ->> 'country'::text))=(au) is duplicated.");
        $adapter = $this->postgresRecording($statements, $duplicates);

        $error = null;
        try {
            $adapter->createIndex('orders', Index::unique(key: 'unique_country', attributes: ['data.country']), ['data.country' => ColumnType::Object->value]);
        } catch (Throwable $caught) {
            $error = $caught;
        }

        $this->assertInstanceOf(UniqueException::class, $error);
        $this->assertSame(UniqueException::MESSAGE, $error->getMessage());
        $this->assertSame($duplicates, $error->getPrevious());
    }

    public function testPostgresDeleteCollectionDropsBothTablesInOneStatement(): void
    {
        $statements = [];
        $adapter = $this->postgresRecording($statements);

        $this->assertTrue($adapter->deleteCollection('orders'));
        $this->assertSame([
            'DROP TABLE "utopiaTests"."engine_orders"; DROP TABLE IF EXISTS "utopiaTests"."engine_orders_perms"',
        ], $statements);
    }

    public function testPostgresDeleteCollectionPassesOtherErrorsThrough(): void
    {
        $statements = [];
        $lockTimeout = self::engineError('55P03', 7, 'SQLSTATE[55P03]: Lock not available: 7 ERROR:  canceling statement due to lock timeout');
        $adapter = $this->postgresRecording($statements, $lockTimeout);

        $error = null;
        try {
            $adapter->deleteCollection('orders');
        } catch (Throwable $caught) {
            $error = $caught;
        }

        $this->assertInstanceOf(TransactionException::class, $error);
        $this->assertSame($lockTimeout, $error->getPrevious());
        $this->assertCount(1, $statements);
    }

    /**
     * @param  Closure(PDOException): Throwable  $map
     * @param  class-string<Throwable>  $expected
     */
    private function assertMapped(Closure $map, PDOException $error, string $expected, string $message): void
    {
        $mapped = $map($error);

        $this->assertInstanceOf($expected, $mapped);
        $this->assertSame($message, $mapped->getMessage());
        $this->assertSame($error, $mapped->getPrevious());
    }

    /**
     * @param  list<Query>  $queries
     */
    private function postgresFindFailure(PDOException $error, array $queries): Throwable
    {
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('execute')->willThrowException($error);
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturn($statement);

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('utopiaTests');
        $adapter->setNamespace(self::NAMESPACE);
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        try {
            $adapter->find(new Document(['$id' => 'orders']), $queries, orderAttributes: ['price'], orderTypes: [OrderDirection::Asc]);
        } catch (Throwable $failure) {
            return $failure;
        }

        $this->fail('The read succeeded');
    }

    /**
     * @param  list<string>  $statements
     */
    private function postgresRecording(array &$statements, ?PDOException $firstError = null): Postgres
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $sql) use (&$statements, $firstError): PDOStatement {
            $statements[] = $sql;
            $statement = $this->createStub(PDOStatement::class);
            if ($firstError !== null && \count($statements) === 1) {
                $statement->method('execute')->willThrowException($firstError);
            } else {
                $statement->method('execute')->willReturn(true);
            }

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('utopiaTests');
        $adapter->setNamespace(self::NAMESPACE);

        return $adapter;
    }

    private static function engineError(string $state, int $code, string $message): PDOException
    {
        $error = new class ($message, $state) extends PDOException {
            public function __construct(string $message, string $state)
            {
                parent::__construct($message);
                $this->code = $state;
            }
        };
        $error->errorInfo = [$state, $code, $message];

        return $error;
    }

    /**
     * @return Closure(PDOException): Throwable
     */
    private static function mariaDB(): Closure
    {
        $adapter = new class (new stdClass()) extends MariaDB {
            public function map(PDOException $error): Throwable
            {
                return $this->processException($error);
            }
        };
        $adapter->setNamespace(self::NAMESPACE);

        return $adapter->map(...);
    }

    /**
     * @return Closure(PDOException): Throwable
     */
    private static function mySQL(): Closure
    {
        $adapter = new class (new stdClass()) extends MySQL {
            public function map(PDOException $error): Throwable
            {
                return $this->processException($error);
            }
        };
        $adapter->setNamespace(self::NAMESPACE);

        return $adapter->map(...);
    }

    /**
     * @return Closure(PDOException): Throwable
     */
    private static function postgres(): Closure
    {
        $adapter = new class (new stdClass()) extends Postgres {
            public function map(PDOException $error): Throwable
            {
                return $this->processException($error);
            }
        };
        $adapter->setNamespace(self::NAMESPACE);

        return $adapter->map(...);
    }

    /**
     * @return Closure(PDOException): Throwable
     */
    private static function sqlite(): Closure
    {
        $adapter = new class (new PDO('sqlite::memory:')) extends SQLite {
            public function map(PDOException $error): Throwable
            {
                return $this->processException($error);
            }
        };
        $adapter->setNamespace(self::NAMESPACE);

        return $adapter->map(...);
    }
}
