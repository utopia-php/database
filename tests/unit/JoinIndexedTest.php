<?php

namespace Tests\Unit;

use ArrayObject;
use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\CursorDirection;

/**
 * Every join read hands the adapter, for each joined collection, the attributes its key and unique indexes lead
 * with: what tells the adapter whether an index serves a join.
 */
final class JoinIndexedTest extends TestCase
{
    private const string CUSTOMERS = 'customers';

    private const string LABELS = 'labels';

    /**
     * @var ArrayObject<int, Document>
     */
    private ArrayObject $collections;

    #[\Override]
    protected function setUp(): void
    {
        $this->collections = new ArrayObject();
    }

    public function testFindHandsTheAdapterTheLeadingAttributesOfEachJoinedCollectionsIndexes(): void
    {
        $database = $this->database();

        $database->find(self::CUSTOMERS, [$this->join()]);

        $this->assertSame([self::LABELS => ['code', 'pair']], $this->indexed());
    }

    public function testCountAndSumHandTheAdapterTheLeadingAttributes(): void
    {
        $database = $this->database();

        $database->count(self::CUSTOMERS, [$this->join()]);
        $this->assertSame([self::LABELS => ['code', 'pair']], $this->indexed());

        $database->sum(self::CUSTOMERS, 'label.score', [$this->join()]);
        $this->assertSame([self::LABELS => ['code', 'pair']], $this->indexed());
    }

    public function testGetDocumentHandsTheAdapterTheLeadingAttributes(): void
    {
        $database = $this->database();

        $database->getDocument(self::CUSTOMERS, 'c1', [$this->join()]);

        $this->assertSame([self::LABELS => ['code', 'pair']], $this->indexed());
    }

    public function testReadWithoutJoinsHandsTheAdapterNoLeadingAttributes(): void
    {
        $database = $this->database();

        $database->find(self::CUSTOMERS, [Query::equal('name', ['one'])]);

        $this->assertNull($this->last()->getAttribute(Database::JOIN_INDEXED));
    }

    private function join(): Query
    {
        return Query::join(self::LABELS, 'label', [Query::on('name', 'name')]);
    }

    /**
     * @return mixed The JOIN_INDEXED attribute of the collection the adapter read last
     */
    private function indexed(): mixed
    {
        return $this->last()->getAttribute(Database::JOIN_INDEXED);
    }

    private function last(): Document
    {
        $collections = $this->collections->getArrayCopy();
        $this->assertNotSame([], $collections, 'The adapter must have read');

        return $collections[\array_key_last($collections)];
    }

    private function database(): Database
    {
        $adapter = new class (new PDO('sqlite::memory:'), $this->collections) extends SQLite {
            /**
             * @param  ArrayObject<int, Document>  $collections
             */
            public function __construct(PDO $pdo, private readonly ArrayObject $collections)
            {
                parent::__construct($pdo);
            }

            #[\Override]
            public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read): array
            {
                $this->collections->append($collection);

                return parent::find($collection, $queries, $limit, $offset, $orderAttributes, $orderTypes, $cursor, $cursorDirection, $forPermission);
            }

            #[\Override]
            public function count(Document $collection, array $queries = [], ?int $max = null): int
            {
                $this->collections->append($collection);

                return parent::count($collection, $queries, $max);
            }

            #[\Override]
            public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): int|float
            {
                $this->collections->append($collection);

                return parent::sum($collection, $attribute, $queries, $max);
            }

            #[\Override]
            public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                $this->collections->append($collection);

                return parent::getDocument($collection, $id, $queries, $forUpdate);
            }
        };

        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database($adapter, new Cache(new None()));
        $database->setDatabase('indexed')->setNamespace('indexed')->setAuthorization($authorization);
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(
            id: self::CUSTOMERS,
            attributes: [Attribute::string('name', 64)],
            permissions: $permissions,
        ));
        $database->createCollection(Collection::create(
            id: self::LABELS,
            attributes: [
                Attribute::string('name', 64),
                Attribute::string('code', 64),
                Attribute::string('pair', 64),
                Attribute::string('text', 256),
                Attribute::integer('score'),
            ],
            indexes: [
                Index::key('code_key', ['code']),
                Index::unique('pair_unique', ['pair', 'name']),
                Index::key('code_name', ['code', 'name']),
                Index::fulltext('text_fulltext', ['text']),
            ],
            permissions: $permissions,
        ));
        $database->createDocument(self::CUSTOMERS, new Document(['$id' => 'c1', 'name' => 'one']));
        $this->collections->exchangeArray([]);

        return $database;
    }
}
