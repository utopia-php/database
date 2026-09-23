<?php

namespace Tests\Unit\Adapter;

use Closure;
use Override;
use Pdo\Sqlite as SQLitePDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Stringable;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Transform;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;

final class QueryCommentsTest extends TestCase
{
    private const string NAMESPACE = 'comments';

    private const string TABLE = self::NAMESPACE.'_movies';

    /**
     * @var list<string>
     */
    private array $statements = [];

    private SQLite $adapter;

    private Database $database;

    protected function setUp(): void
    {
        $this->adapter = new SQLite($this->recordingConnection());
        $this->database = $this->open($this->adapter);
        $this->database->create();
        $this->database->createCollection(new Collection(
            id: 'movies',
            attributes: [
                Attribute::string('title', size: 128),
                Attribute::integer('year'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ));
        $this->database->createDocument('movies', new Document([
            '$id' => 'dune',
            'title' => 'Dune',
            'year' => 1965,
        ]));

        $this->statements = [];
    }

    public function testTransformSeesTheCommentsAheadOfTheStatementInInsertionOrder(): void
    {
        $transform = new class () implements Transform {
            /**
             * @var array<string, string>
             */
            public array $queries = [];

            public function transform(Event $event, string $query): string
            {
                $this->queries[$event->value] = $query;

                return $query;
            }
        };
        $this->database->addHook($transform);

        $this->database
            ->setMetadata('host', 'worker-1')
            ->setMetadata('project', 'console')
            ->setMetadata('user', 'user-1')
            ->setMetadata('host', 'worker-2');

        $this->assertSame('Dune', $this->database->getDocument('movies', 'dune')->getAttribute('title'));
        $this->assertStringStartsWith(
            "/* host: worker-2 */\n/* project: console */\n/* user: user-1 */\nSELECT ",
            $transform->queries[Event::DocumentRead->value] ?? '',
        );
    }

    public function testResetMetadataRemovesTheComments(): void
    {
        $this->database->setMetadata('user', 'user-1');
        $this->assertCount(1, $this->database->find('movies'));
        $this->assertEveryStatementStartsWith("/* user: user-1 */\n");

        $this->database->resetMetadata();
        $this->statements = [];

        $this->assertCount(1, $this->database->find('movies'));
        $this->assertNotEmpty($this->statements);
        foreach ($this->statements as $statement) {
            $this->assertStringStartsNotWith('/*', $statement);
        }
    }

    public function testStatementsPreparedWithoutAnEventCarryTheComments(): void
    {
        $this->database->setMetadata('user', 'user-1');

        $this->assertTrue($this->database->ping());
        $rows = $this->database->rawQuery('SELECT ? AS answer', [42]);

        $this->assertSame(42, $rows[0]->getAttribute('answer'));
        $this->assertSame([
            "/* user: user-1 */\nSELECT 1",
            "/* user: user-1 */\nSELECT ? AS answer",
        ], $this->statements);
    }

    /**
     * @return array<string, array{class-string<MariaDB|Postgres>, string}>
     */
    public static function connectionIdFunctions(): array
    {
        return [
            'MariaDB' => [MariaDB::class, 'CONNECTION_ID'],
            'MySQL' => [MySQL::class, 'CONNECTION_ID'],
            'Postgres' => [Postgres::class, 'pg_backend_pid'],
        ];
    }

    /**
     * @param class-string<MariaDB|Postgres> $class
     */
    #[DataProvider('connectionIdFunctions')]
    public function testConnectionIdQueryCarriesTheComments(string $class, string $function): void
    {
        $connection = $this->recordingConnection();
        $connection->createFunction($function, static fn (): int => 7, 0);
        $adapter = new $class($connection);
        $adapter->setMetadata('user', 'user-1');

        $this->assertSame('7', $adapter->getConnectionId());
        $this->assertSame(["/* user: user-1 */\nSELECT {$function}()"], $this->statements);
    }

    public function testEveryStatementCarriesTheComments(): void
    {
        $this->database->setMetadata('user', 'user-1');

        $this->database->createAttribute('movies', Attribute::string('director', size: 64));
        $this->database->createIndex('movies', Index::key('year_index', ['year']));
        $this->database->createDocument('movies', new Document([
            '$id' => 'arrival',
            'title' => 'Arrival',
            'year' => 2016,
            'director' => 'Villeneuve',
        ]));
        $this->database->withTransaction(fn (): Document => $this->database->updateDocument(
            'movies',
            'dune',
            new Document(['director' => 'Villeneuve']),
        ));
        $this->database->increaseDocumentAttribute('movies', 'arrival', 'year', 1);

        $this->assertCount(2, $this->database->find('movies', [Query::equal('director', ['Villeneuve'])]));
        $this->assertSame(2, $this->database->count('movies'));
        $this->assertSame(1965 + 2017, $this->database->sum('movies', 'year'));
        $this->assertTrue($this->database->deleteDocument('movies', 'arrival'));
        $this->assertTrue($this->database->deleteCollection('movies'));

        $this->assertEveryStatementStartsWith("/* user: user-1 */\n");
    }

    /**
     * @return array<string, array{string, string, string}>
     */
    public static function delimiterMetadata(): array
    {
        return [
            'value closing the comment' => ['user', 'user-1 */ tail', '/* user: user-1 * / tail */'],
            'key closing the comment' => ['user */ tail', 'user-1', '/* user * / tail: user-1 */'],
            'value opening a nested comment' => ['user', '/* user-1', '/* user: / * user-1 */'],
            'key opening a nested comment' => ['/* user', 'user-1', '/* / * user: user-1 */'],
            'overlapping delimiters' => ['user', '/*/*//**/', '/* user: / * / * // ** / */'],
            'line breaks' => ['user', "user-1\r\n*/ tail", '/* user: user-1  * / tail */'],
            'NUL byte' => ['user', "user-1\0*/", '/* user: user-1 * / */'],
            'unicode line separator' => ['user', "user\u{2028}1", '/* user: user 1 */'],
            'invalid UTF-8' => ['user', "user-\xB1", '/* user: user-? */'],
            'placeholders and quotes' => ['user', "O'Brien :year ? \"1\"", "/* user: O'Brien :year ? \"1\" */"],
        ];
    }

    #[DataProvider('delimiterMetadata')]
    public function testMetadataStaysInsideItsComment(string $key, string $value, string $comment): void
    {
        $this->database->setMetadata($key, $value);

        $this->database->createDocument('movies', new Document([
            '$id' => 'arrival',
            'title' => 'Arrival',
            'year' => 2016,
        ]));
        $this->database->updateDocument('movies', 'dune', new Document(['year' => 2021]));

        $this->assertSame('Arrival', $this->database->getDocument('movies', 'arrival')->getAttribute('title'));
        $this->assertCount(1, $this->database->find('movies', [Query::equal('year', [2021])]));
        $this->assertSame(2, $this->database->count('movies'));
        $this->assertEveryStatementStartsWith($comment."\n");

        $this->database->resetMetadata();

        $this->assertSame(2021, $this->database->getDocument('movies', 'dune')->getAttribute('year'));
        $this->assertCount(2, $this->database->find('movies'));
    }

    public function testMetadataValuesAreRenderedAsText(): void
    {
        $stringable = new class () implements Stringable {
            public function __toString(): string
            {
                return 'region */ one';
            }
        };

        $this->database
            ->setMetadata('integer', 42)
            ->setMetadata('float', 1.5)
            ->setMetadata('true', true)
            ->setMetadata('false', false)
            ->setMetadata('null', null)
            ->setMetadata('list', ['a', 'b*/'])
            ->setMetadata('map', ['path' => 'a/b', 'id' => 7])
            ->setMetadata('stringable', $stringable)
            ->setMetadata('object', (object) ['user' => "user\n1"]);

        $this->assertTrue($this->database->ping());
        $this->assertSame([
            "/* integer: 42 */\n"
            ."/* float: 1.5 */\n"
            ."/* true: 1 */\n"
            ."/* false:  */\n"
            ."/* null: null */\n"
            ."/* list: [\"a\",\"b* /\"] */\n"
            ."/* map: {\"path\":\"a/b\",\"id\":7} */\n"
            ."/* stringable: region * / one */\n"
            ."/* object: {\"user\":\"user\\n1\"} */\n"
            .'SELECT 1',
        ], $this->statements);
    }

    public function testPooledConnectionCarriesOnlyTheCurrentHandlesMetadata(): void
    {
        $first = $this->open($this->pool());
        $second = $this->open($this->pool());

        $first->setMetadata('user', 'user-1');

        $this->assertTrue($first->ping());
        $this->assertTrue($second->ping());
        $this->assertSame(["/* user: user-1 */\nSELECT 1", 'SELECT 1'], $this->statements);
    }

    /**
     * @param non-empty-string $prefix
     */
    private function assertEveryStatementStartsWith(string $prefix): void
    {
        $this->assertNotEmpty($this->statements);
        foreach ($this->statements as $statement) {
            $this->assertStringStartsWith($prefix, $statement);
        }
    }

    private function open(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new NoCache()));
        $database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());

        return $database;
    }

    private function pool(): Pool
    {
        return new Pool(new UtopiaPool(new Stack(), 'sqlite', 1, fn (): SQLite => $this->adapter, timeout: 0.0));
    }

    private function recordingConnection(): SQLitePDO
    {
        $record = function (string $statement): void {
            $this->statements[] = $statement;
        };

        return new class ($record) extends SQLitePDO {
            public function __construct(private readonly Closure $record)
            {
                parent::__construct('sqlite::memory:');
            }

            /**
             * @param array<mixed> $options
             */
            #[Override]
            public function prepare(string $query, array $options = []): PDOStatement|false
            {
                ($this->record)($query);

                return parent::prepare($query, $options);
            }
        };
    }
}
