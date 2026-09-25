<?php

namespace Tests\Unit\Joins;

use ArrayObject;
use PDO;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * A joined attribute comes back as a direct read of its collection returns it: cast to its type and
 * passed through every decode filter it declares, whether the join projects it or a select names it.
 */
final class JoinedDecodeTest extends TestCase
{
    private const string CIPHER = 'aes-128-gcm';

    private const int IV_LENGTH = 12;

    private const string KEY = 'joined-decode-test-key';

    private const string WITNESS_PREFIX = 'witnessed:';

    private const array ORDER_ATTRIBUTES = ['customerId', 'total', 'price', 'paid', 'placedAt', 'tags', 'meta', 'secret', 'profile', 'note'];

    private const array REFUND_ATTRIBUTES = ['customerId', 'total', 'secret'];

    /**
     * @var ArrayObject<int, array{document: Document, database: Database}>
     */
    private ArrayObject $witnessed;

    protected function setUp(): void
    {
        $this->witnessed = new ArrayObject();
    }

    public function testImplicitProjectionDecodesLikeADirectRead(): void
    {
        $database = $this->database();

        $rows = $database->find('customers', [Query::join('orders', '$id', 'customerId', '=', 'ord')]);

        $this->assertCount(1, $rows);
        $this->assertDecodedOrder($rows[0], 'ord');
        $this->assertLikeDirectRead($database, $rows[0], 'ord', 'orders', 'o1', ['$id', ...self::ORDER_ATTRIBUTES]);
    }

    public function testExplicitSelectDecodesLikeADirectRead(): void
    {
        $database = $this->database();

        $rows = $database->find('customers', [
            Query::join('orders', '$id', 'customerId', '=', 'ord'),
            Query::select(['name', ...\array_map(static fn (string $attribute): string => 'ord.'.$attribute, self::ORDER_ATTRIBUTES)]),
        ]);

        $this->assertCount(1, $rows);
        $this->assertDecodedOrder($rows[0], 'ord');
        $this->assertLikeDirectRead($database, $rows[0], 'ord', 'orders', 'o1', self::ORDER_ATTRIBUTES);
    }

    public function testSelectingOneJoinedAttributeDecodesOnlyThatAttribute(): void
    {
        $database = $this->database();

        $rows = $database->find('customers', [
            Query::join('orders', '$id', 'customerId', '=', 'ord'),
            Query::select(['name', 'ord.secret']),
        ]);

        $this->assertCount(1, $rows);
        $this->assertSame('plain-secret', $rows[0]->getAttribute('ord.secret'));
        $joined = \array_values(\array_filter(
            \array_map(\strval(...), \array_keys($rows[0]->getArrayCopy())),
            static fn (string $key): bool => \str_starts_with($key, 'ord.'),
        ));
        $this->assertSame(['ord.secret'], $joined);
    }

    public function testGetDocumentDecodesLikeADirectRead(): void
    {
        $database = $this->database();
        $join = Query::join('orders', '$id', 'customerId', '=', 'ord');

        $implicit = $database->getDocument('customers', 'c1', [$join]);
        $this->assertDecodedOrder($implicit, 'ord');
        $this->assertLikeDirectRead($database, $implicit, 'ord', 'orders', 'o1', ['$id', ...self::ORDER_ATTRIBUTES]);

        $selected = $database->getDocument('customers', 'c1', [
            $join,
            Query::select(['name', ...\array_map(static fn (string $attribute): string => 'ord.'.$attribute, self::ORDER_ATTRIBUTES)]),
        ]);
        $this->assertDecodedOrder($selected, 'ord');
        $this->assertLikeDirectRead($database, $selected, 'ord', 'orders', 'o1', self::ORDER_ATTRIBUTES);
    }

    public function testEachAliasDecodesWithItsOwnCollection(): void
    {
        $database = $this->database();

        $rows = $database->find('customers', [
            Query::join('orders', '$id', 'customerId', '=', 'ord'),
            Query::join('refunds', '$id', 'customerId', '=', 'ref'),
        ]);

        $this->assertCount(1, $rows);
        $this->assertLikeDirectRead($database, $rows[0], 'ord', 'orders', 'o1', ['$id', ...self::ORDER_ATTRIBUTES]);
        $this->assertLikeDirectRead($database, $rows[0], 'ref', 'refunds', 'r1', ['$id', ...self::REFUND_ATTRIBUTES]);
        $this->assertSame(7.5, $rows[0]->getAttribute('ref.total'));
        $this->assertSame('refund-note', $rows[0]->getAttribute('ref.secret'));
    }

    public function testGeneratedAliasesDecodeWithTheirJoinedCollection(): void
    {
        $database = $this->database();

        $generated = $database->find('customers', [Query::join('orders', '$id', 'customerId')]);
        $this->assertCount(1, $generated);
        $this->assertDecodedOrder($generated[0], 'j0');
        $this->assertLikeDirectRead($database, $generated[0], 'j0', 'orders', 'o1', ['$id', ...self::ORDER_ATTRIBUTES]);

        $skipping = $database->find('customers', [
            Query::join('orders', '$id', 'customerId'),
            Query::join('refunds', '$id', 'customerId', '=', 'j0'),
        ]);
        $this->assertCount(1, $skipping);
        $this->assertDecodedOrder($skipping[0], 'j1');
        $this->assertLikeDirectRead($database, $skipping[0], 'j1', 'orders', 'o1', ['$id', ...self::ORDER_ATTRIBUTES]);
        $this->assertLikeDirectRead($database, $skipping[0], 'j0', 'refunds', 'r1', ['$id', ...self::REFUND_ATTRIBUTES]);
    }

    public function testJoinedInternalAttributesDecodeLikeADirectRead(): void
    {
        $database = $this->database();

        $rows = $database->find('customers', [
            Query::join('orders', '$id', 'customerId', '=', 'ord'),
            Query::select(['name', 'ord.$id', 'ord.$sequence', 'ord.$createdAt', 'ord.$updatedAt', 'ord.$permissions']),
        ]);

        $this->assertCount(1, $rows);
        $this->assertLikeDirectRead($database, $rows[0], 'ord', 'orders', 'o1', ['$id', '$sequence', '$createdAt', '$updatedAt', '$permissions']);
    }

    public function testDecodeFiltersReceiveTheJoinedDocument(): void
    {
        $database = $this->database();
        $join = Query::join('orders', '$id', 'customerId', '=', 'ord');

        $this->witnessed->exchangeArray([]);
        $database->find('customers', [$join]);
        $database->getDocument('customers', 'c1', [$join]);

        $this->assertCount(2, $this->witnessed);
        foreach ($this->witnessed as ['document' => $document, 'database' => $witness]) {
            $this->assertSame('o1', $document->getId());
            $this->assertSame('orders', $document->getCollection());
            $this->assertSame('c1', $document->getAttribute('customerId'));
            $this->assertSame(10, $document->getAttribute('total'));
            $this->assertSame($database, $witness);
        }
    }

    public function testUnmatchedJoinReturnsNullValues(): void
    {
        $database = $this->database();

        $this->witnessed->exchangeArray([]);
        $rows = $database->find('customers', [
            Query::leftJoin('orders', '$id', 'customerId', '=', 'ord'),
            Query::orderAsc('name'),
        ]);

        $this->assertSame(['Alice', 'Bob'], \array_map(static fn (Document $row): mixed => $row->getAttribute('name'), $rows));
        $this->assertDecodedOrder($rows[0], 'ord');
        $this->assertNull($rows[1]->getAttribute('ord.$id'));
        foreach (self::ORDER_ATTRIBUTES as $attribute) {
            $this->assertArrayHasKey('ord.'.$attribute, $rows[1]->getArrayCopy());
            $this->assertNull($rows[1]->getAttribute('ord.'.$attribute), 'ord.'.$attribute);
        }
        $this->assertCount(1, $this->witnessed, 'A decode filter only runs for a joined row that exists');
    }

    public function testCursorAfterARowPagesByAJoinedDatetime(): void
    {
        $database = $this->database();
        foreach (['c3' => '2024-05-06T08:00:00.000+00:00', 'c4' => '2024-05-06T09:00:00.000+00:00'] as $customer => $time) {
            $database->createDocument('customers', new Document(['$id' => $customer, 'name' => $customer]));
            $database->createDocument('orders', $this->order('o'.$customer, $customer, $time));
        }

        $queries = [
            Query::join('orders', '$id', 'customerId', '=', 'ord'),
            Query::orderAsc('ord.placedAt'),
            Query::limit(1),
        ];

        $customers = [];
        $cursor = null;
        $placedAt = null;
        for ($page = 0; $page < 4; $page++) {
            $rows = $database->find('customers', $cursor === null ? $queries : [...$queries, Query::cursorAfter($cursor)]);
            if ($rows === []) {
                break;
            }

            $customers[] = $rows[0]->getId();
            if ($cursor !== null) {
                $this->assertSame($placedAt, $cursor->getAttribute('ord.placedAt'), 'The cursor document keeps its decoded values');
            }
            $cursor = $rows[0];
            $placedAt = $cursor->getAttribute('ord.placedAt');
        }

        $this->assertSame(['c1', 'c3', 'c4'], $customers);
    }

    private function assertDecodedOrder(Document $row, string $alias): void
    {
        $this->assertSame('c1', $row->getAttribute($alias.'.customerId'));
        $this->assertSame(10, $row->getAttribute($alias.'.total'));
        $this->assertSame(2.5, $row->getAttribute($alias.'.price'));
        $this->assertTrue($row->getAttribute($alias.'.paid'));
        $this->assertSame(['a', 'b'], $row->getAttribute($alias.'.tags'));
        $this->assertSame(['color' => 'red'], $row->getAttribute($alias.'.meta'));
        $this->assertSame('plain-secret', $row->getAttribute($alias.'.secret'));
        $this->assertSame(['tier' => 'gold'], $row->getAttribute($alias.'.profile'));
        $this->assertSame('handwritten', $row->getAttribute($alias.'.note'));
    }

    /**
     * @param  list<string>  $attributes
     */
    private function assertLikeDirectRead(Database $database, Document $row, string $alias, string $collection, string $id, array $attributes): void
    {
        $direct = $database->getDocument($collection, $id);
        $this->assertFalse($direct->isEmpty());

        foreach ($attributes as $attribute) {
            $this->assertSame($direct->getAttribute($attribute), $row->getAttribute($alias.'.'.$attribute), $alias.'.'.$attribute);
        }
    }

    private function database(): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $connection = new PDO('sqlite::memory:', null, null, [PDO::ATTR_PERSISTENT => false] + SQLite::getPDOAttributes());
        $database = new Database(new SQLite($connection), new Cache(new None()), $this->filters());
        $database
            ->setAuthorization($authorization)
            ->setDatabase('joined_decode')
            ->setNamespace('joined_decode_'.\uniqid());
        $database->addHook(new Permissions());
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(new Collection(
            id: 'customers',
            attributes: [Attribute::string(key: 'name', size: 64, required: true)],
            permissions: $permissions,
            documentSecurity: false,
        ));
        $database->createCollection(new Collection(
            id: 'orders',
            attributes: [
                Attribute::string(key: 'customerId', size: 64, required: true),
                Attribute::integer(key: 'total', required: true),
                Attribute::float(key: 'price', required: true),
                Attribute::boolean(key: 'paid', required: true),
                Attribute::datetime(key: 'placedAt', required: true),
                Attribute::string(key: 'tags', size: 32, array: true),
                Attribute::string(key: 'meta', size: 1024, filters: ['json']),
                Attribute::string(key: 'secret', size: 1024, filters: ['sealed']),
                Attribute::string(key: 'profile', size: 1024, filters: ['json', 'sealed']),
                Attribute::string(key: 'note', size: 256, filters: ['witness']),
            ],
            permissions: $permissions,
            documentSecurity: false,
        ));
        $database->createCollection(new Collection(
            id: 'refunds',
            attributes: [
                Attribute::string(key: 'customerId', size: 64, required: true),
                Attribute::float(key: 'total', required: true),
                Attribute::string(key: 'secret', size: 64, required: true),
            ],
            permissions: $permissions,
            documentSecurity: false,
        ));

        $database->createDocument('customers', new Document(['$id' => 'c1', 'name' => 'Alice']));
        $database->createDocument('customers', new Document(['$id' => 'c2', 'name' => 'Bob']));
        $database->createDocument('orders', $this->order('o1', 'c1', '2024-05-06T07:08:09.123+00:00'));
        $database->createDocument('refunds', new Document([
            '$id' => 'r1',
            'customerId' => 'c1',
            'total' => 7.5,
            'secret' => 'refund-note',
        ]));

        return $database;
    }

    private function order(string $id, string $customerId, string $placedAt): Document
    {
        return new Document([
            '$id' => $id,
            'customerId' => $customerId,
            'total' => 10,
            'price' => 2.5,
            'paid' => true,
            'placedAt' => $placedAt,
            'tags' => ['a', 'b'],
            'meta' => ['color' => 'red'],
            'secret' => 'plain-secret',
            'profile' => ['tier' => 'gold'],
            'note' => 'handwritten',
        ]);
    }

    /**
     * @return array<string, array{encode: callable, decode: callable}>
     */
    private function filters(): array
    {
        return [
            'sealed' => [
                'encode' => static function (mixed $value): mixed {
                    if (! \is_string($value)) {
                        return $value;
                    }

                    $iv = \random_bytes(self::IV_LENGTH);
                    $tag = '';
                    $data = \openssl_encrypt($value, self::CIPHER, self::KEY, 0, $iv, $tag);

                    return \json_encode([
                        'data' => $data,
                        'method' => self::CIPHER,
                        'iv' => \bin2hex($iv),
                        'tag' => \bin2hex($tag),
                        'version' => '1',
                    ]);
                },
                'decode' => static function (mixed $value): mixed {
                    if ($value === null) {
                        return null;
                    }

                    $payload = \is_string($value) ? \json_decode($value, true) : null;
                    if (
                        ! \is_array($payload)
                        || ! \is_string($payload['data'] ?? null)
                        || ! \is_string($payload['iv'] ?? null)
                        || ! \is_string($payload['tag'] ?? null)
                    ) {
                        throw new RuntimeException('Not a sealed value: '.\var_export($value, true));
                    }

                    return \openssl_decrypt($payload['data'], self::CIPHER, self::KEY, 0, (string) \hex2bin($payload['iv']), (string) \hex2bin($payload['tag']));
                },
            ],
            'witness' => [
                'encode' => static fn (mixed $value): mixed => \is_string($value) ? self::WITNESS_PREFIX.$value : $value,
                'decode' => function (mixed $value, Document $document, Database $database): mixed {
                    $this->witnessed->append(['document' => clone $document, 'database' => $database]);

                    return \is_string($value) && \str_starts_with($value, self::WITNESS_PREFIX)
                        ? \substr($value, \strlen(self::WITNESS_PREFIX))
                        : $value;
                },
            ],
        ];
    }
}
