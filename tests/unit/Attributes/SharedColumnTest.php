<?php

namespace Tests\Unit\Attributes;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

final class SharedColumnTest extends TestCase
{
    private const string COLLECTION = 'people';

    public const string KEY = 'age';

    private const string DOCUMENT = 'first';

    private const int OVERSIZED_NAME_LENGTH = 257;

    private string $path;

    private string $namespace;

    protected function setUp(): void
    {
        $this->path = \sys_get_temp_dir().'/shared_column_'.\uniqid().'.sqlite';
        $this->namespace = 'shared_column_'.\uniqid();
    }

    protected function tearDown(): void
    {
        if (\is_file($this->path)) {
            \unlink($this->path);
        }
    }

    public function testAnotherTenantsColumnOfAnotherTypeIsNeverDropped(): void
    {
        $first = $this->tenantWithAge(1);
        $second = $this->tenant(2);

        $refusal = $this->refusal(fn (): bool => $second->createAttribute(self::COLLECTION, Attribute::string(key: self::KEY, size: 64)));

        $this->assertSame(7, $first->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute(self::KEY));
        $this->assertRefusedAsAnotherType($refusal);
        $this->assertSame([], $this->keys($second));
    }

    public function testAnotherTenantsColumnOfTheSameTypeIsReused(): void
    {
        $first = $this->tenantWithAge(1);
        $second = $this->tenant(2);

        $this->assertTrue($second->createAttribute(self::COLLECTION, Attribute::integer(key: self::KEY)));

        $this->assertSame(7, $first->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute(self::KEY));
        $this->assertSame([self::KEY], $this->keys($second));
        $second->createDocument(self::COLLECTION, new Document([Document::ID => 'second', self::KEY => 9]));
        $this->assertSame(9, $second->getDocument(self::COLLECTION, 'second')->getAttribute(self::KEY));
    }

    public function testCreateAttributesNeverDropsAnotherTenantsColumnOfAnotherType(): void
    {
        $first = $this->tenantWithAge(1);
        $second = $this->tenant(2);

        $refusal = $this->refusal(fn (): bool => $second->createAttributes(self::COLLECTION, [
            Attribute::string(key: 'nick', size: 16),
            Attribute::string(key: self::KEY, size: 64),
        ]));

        $this->assertSame(7, $first->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute(self::KEY));
        $this->assertRefusedAsAnotherType($refusal);
        $this->assertSame([], $this->keys($second));
        $this->assertNotContains('nick', $this->columns($first));
    }

    public function testCreateAttributesReusesAnotherTenantsColumnOfTheSameType(): void
    {
        $first = $this->tenantWithAge(1);
        $second = $this->tenant(2);

        $this->assertTrue($second->createAttributes(self::COLLECTION, [
            Attribute::integer(key: self::KEY),
            Attribute::string(key: 'nick', size: 16),
        ]));

        $this->assertSame(7, $first->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute(self::KEY));
        $this->assertSame([self::KEY, 'nick'], $this->keys($second));
    }

    public function testCreateAttributesRollbackKeepsAnotherTenantsReusedColumn(): void
    {
        $first = $this->tenantWithAge(1);
        $second = $this->tenant(2, \str_repeat('n', self::OVERSIZED_NAME_LENGTH));

        try {
            $second->createAttributes(self::COLLECTION, [
                Attribute::integer(key: self::KEY),
                Attribute::string(key: 'nick', size: 16),
            ]);
            $this->fail('The metadata write of an oversized collection name must fail');
        } catch (DatabaseException $error) {
            $this->assertInstanceOf(StructureException::class, $error->getPrevious());
        }

        $this->assertSame(7, $first->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute(self::KEY));
        $this->assertNotContains('nick', $this->columns($first));
    }

    /**
     * @return array<string, array{Attribute, string}>
     */
    public static function engineSpellings(): array
    {
        return [
            'integer on MariaDB' => [Attribute::integer(key: self::KEY), 'int(11)'],
            'integer on MySQL' => [Attribute::integer(key: self::KEY), 'int'],
            'unsigned integer on MariaDB' => [Attribute::integer(key: self::KEY, signed: false), 'int(10) unsigned'],
            'big integer on MariaDB' => [Attribute::bigInteger(key: self::KEY), 'bigint(20)'],
            'boolean' => [Attribute::boolean(key: self::KEY), 'tinyint(1)'],
            'double' => [Attribute::double(key: self::KEY), 'double'],
            'datetime' => [Attribute::datetime(key: self::KEY), 'datetime(3)'],
            'string' => [Attribute::string(key: self::KEY, size: 64), 'varchar(64)'],
            'array on MariaDB' => [Attribute::string(key: self::KEY, size: 64, array: true), 'longtext'],
            'array on MySQL' => [Attribute::string(key: self::KEY, size: 64, array: true), 'json'],
        ];
    }

    #[DataProvider('engineSpellings')]
    public function testAnotherTenantsColumnInTheEngineSpellingIsReused(Attribute $attribute, string $reported): void
    {
        $first = $this->tenantWithAge(1);
        $second = $this->tenant(2, adapter: $this->reporting($reported));

        $this->assertTrue($second->createAttribute(self::COLLECTION, $attribute));

        $this->assertSame(7, $first->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute(self::KEY));
        $this->assertSame([self::KEY], $this->keys($second));
    }

    /**
     * @return array<string, array{Attribute, string}>
     */
    public static function conflictingSpellings(): array
    {
        return [
            'integer over big integer' => [Attribute::integer(key: self::KEY), 'bigint(20)'],
            'signed over unsigned' => [Attribute::integer(key: self::KEY), 'int(10) unsigned'],
            'string over integer' => [Attribute::string(key: self::KEY, size: 64), 'int(11)'],
            'string over a longer string' => [Attribute::string(key: self::KEY, size: 64), 'varchar(128)'],
        ];
    }

    #[DataProvider('conflictingSpellings')]
    public function testAnotherTenantsColumnInAConflictingEngineSpellingIsRefused(Attribute $attribute, string $reported): void
    {
        $first = $this->tenantWithAge(1);
        $second = $this->tenant(2, adapter: $this->reporting($reported));

        $refusal = $this->refusal(fn (): bool => $second->createAttribute(self::COLLECTION, $attribute));

        $this->assertSame(7, $first->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute(self::KEY));
        $this->assertRefusedAsAnotherType($refusal);
        $this->assertSame([], $this->keys($second));
    }

    /**
     * @param  callable(): bool  $operation
     */
    private function refusal(callable $operation): ?DuplicateException
    {
        try {
            $operation();
        } catch (DuplicateException $error) {
            return $error;
        }

        return null;
    }

    private function assertRefusedAsAnotherType(?DuplicateException $refusal): void
    {
        $this->assertNotNull($refusal, 'A column another tenant stores with another type must be refused');
        $this->assertSame('Attribute exists in the shared table with another type', $refusal->getMessage());
    }

    private function tenantWithAge(int $tenant): Database
    {
        $database = $this->tenant($tenant);
        $database->createAttribute(self::COLLECTION, Attribute::integer(key: self::KEY));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => self::DOCUMENT, self::KEY => 7]));

        return $database;
    }

    private function tenant(int $tenant, string $name = self::COLLECTION, ?SQLite $adapter = null): Database
    {
        $database = new Database($adapter ?? new SQLite(new PDO('sqlite:'.$this->path)), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('shared_column')
            ->setNamespace($this->namespace)
            ->setSharedTables(true)
            ->setTenant($tenant);
        $database->getAuthorization()->addRole(Role::any()->toString());

        if (! $database->exists()) {
            $database->create();
        }

        $collection = new Collection(
            id: self::COLLECTION,
            name: $name,
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
            documentSecurity: false,
        );
        $database->skipValidation(fn (): Collection => $database->createCollection($collection));

        return $database;
    }

    private function reporting(string $columnType): SQLite
    {
        return new class (new PDO('sqlite:'.$this->path), $columnType) extends SQLite {
            public function __construct(PDO $pdo, private readonly string $columnType)
            {
                parent::__construct($pdo);
            }

            public function getSchemaAttributes(string $collection): array
            {
                return \array_map(
                    fn (Document $column): Document => $column->getId() === SharedColumnTest::KEY
                        ? $column->setAttribute('columnType', $this->columnType)
                        : $column,
                    parent::getSchemaAttributes($collection),
                );
            }
        };
    }

    /**
     * @return list<string>
     */
    private function keys(Database $database): array
    {
        return \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            \array_values($database->getCollection(self::COLLECTION)->attributes),
        );
    }

    /**
     * @return list<string>
     */
    private function columns(Database $database): array
    {
        return \array_map(
            static fn (Document $column): string => $column->getId(),
            $database->getSchemaAttributes(self::COLLECTION),
        );
    }
}
