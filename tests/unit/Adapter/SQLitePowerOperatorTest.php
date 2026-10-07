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
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class SQLitePowerOperatorTest extends TestCase
{
    private const string NAMESPACE = 'power';

    private SQLite $adapter;

    private Database $database;

    protected function setUp(): void
    {
        $this->adapter = new SQLite(new PDO('sqlite::memory:'));
        $this->database = new Database($this->adapter, new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $this->database->create();
        $this->database->createCollection(Collection::create(
            id: 'scores',
            attributes: [Attribute::integer('value')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
        $this->database->createDocument('scores', new Document(['$id' => 'first', 'value' => 3]));
    }

    /**
     * @return iterable<string, array{mixed}>
     */
    public static function nonNumericExponents(): iterable
    {
        yield 'word' => ['two'];
        yield 'boolean' => [true];
        yield 'list' => [[2]];
    }

    #[DataProvider('nonNumericExponents')]
    public function testPowerWithANonNumericExponentIsRefusedAndLeavesTheValue(mixed $exponent): void
    {
        try {
            $this->adapter->updateDocuments(
                $this->database->getCollection('scores'),
                new Document(['value' => new Operator(OperatorType::Power, 'value', [$exponent])]),
                [$this->database->getDocument('scores', 'first')],
            );
            $this->fail('A power exponent that is not a number must be refused');
        } catch (OperatorException $error) {
            $this->assertSame('Power exponent must be numeric', $error->getMessage());
        }

        $this->assertSame(3, $this->database->getDocument('scores', 'first')->getAttribute('value'));
    }

    public function testPowerWithANumericExponentRaisesTheValue(): void
    {
        $this->database->updateDocument('scores', 'first', new Document(['value' => Operator::power(2)]));

        $this->assertSame(9, $this->database->getDocument('scores', 'first')->getAttribute('value'));
    }
}
