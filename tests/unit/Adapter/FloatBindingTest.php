<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Operator;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

final class FloatBindingTest extends TestCase
{
    /**
     * @var list<mixed>
     */
    private array $bindings = [];

    /**
     * @return array<string, array{float, string}>
     */
    public static function floats(): array
    {
        return [
            'sum' => [0.1 + 0.2, '0.30000000000000004'],
            'pi' => [M_PI, '3.141592653589793'],
            'seventeen digits' => [123456789.12345679, '123456789.12345679'],
            'denormal' => [5e-324, '4.9406564584125E-324'],
            'tiny' => [1e-300, '1.0E-300'],
            'negative' => [-2.5e-20, '-2.5E-20'],
            'max' => [1.7976931348623157e308, '1.7976931348623157E+308'],
            'short' => [0.1, '0.1'],
        ];
    }

    /**
     * @return array<string, array{class-string<SQL>}>
     */
    public static function adapters(): array
    {
        return [
            'mariadb' => [MariaDB::class],
            'mysql' => [MySQL::class],
            'postgres' => [Postgres::class],
            'sqlite' => [SQLite::class],
        ];
    }

    /**
     * @return array<string, array{class-string<SQL>, float, string}>
     */
    public static function adaptersAndFloats(): array
    {
        $cases = [];
        foreach (self::adapters() as $adapter => [$class]) {
            foreach (self::floats() as $name => [$value, $bound]) {
                $cases[$adapter.' '.$name] = [$class, $value, $bound];
            }
        }

        return $cases;
    }

    /**
     * @param  class-string<SQL>  $class
     */
    #[DataProvider('adaptersAndFloats')]
    public function testACreateBindsADecimalThatRoundTrips(string $class, float $value, string $bound): void
    {
        $this->adapter($class)->createDocument($this->collection(), new Document(['$id' => 'one', '$permissions' => [], 'f' => $value]));

        $this->assertBound($value, $bound);
    }

    /**
     * @param  class-string<SQL>  $class
     */
    #[DataProvider('adaptersAndFloats')]
    public function testAnUpdateBindsADecimalThatRoundTrips(string $class, float $value, string $bound): void
    {
        $collection = $this->collection();
        $this->adapter($class)->updateDocuments($collection, new Document(['f' => $value]), [new Document(['$id' => 'one', '$sequence' => '1', '$permissions' => []])]);

        $this->assertBound($value, $bound);
    }

    /**
     * @param  class-string<SQL>  $class
     */
    #[DataProvider('adaptersAndFloats')]
    public function testAnIncrementBindsADecimalThatRoundTrips(string $class, float $value, string $bound): void
    {
        $this->adapter($class)->increaseDocumentAttribute($this->collection(), 'one', 'f', $value, '2026-01-01 00:00:00.000');

        $this->assertBound($value, $bound);
    }

    /**
     * @param  class-string<SQL>  $class
     */
    #[DataProvider('adaptersAndFloats')]
    public function testAnIncrementOperatorBindsADecimalThatRoundTrips(string $class, float $value, string $bound): void
    {
        $collection = $this->collection();
        $this->adapter($class)->updateDocuments($collection, new Document(['f' => Operator::increment($value)]), [new Document(['$id' => 'one', '$sequence' => '1', '$permissions' => []])]);

        $this->assertBound($value, $bound);
    }

    /**
     * @param  class-string<SQL>  $class
     */
    #[DataProvider('adaptersAndFloats')]
    public function testFindCountAndSumBindAFilterAlike(string $class, float $value, string $bound): void
    {
        $adapter = $this->adapter($class);
        $collection = $this->collection();
        $queries = [Query::equal('f', [$value])];

        $adapter->find($collection, $queries);
        $this->assertBound($value, $bound);

        $this->bindings = [];
        $adapter->count($collection, $queries);
        $this->assertBound($value, $bound);

        $this->bindings = [];
        $adapter->sum($collection, 'f', $queries);
        $this->assertBound($value, $bound);
    }

    /**
     * @param  class-string<SQL>  $class
     */
    #[DataProvider('adapters')]
    public function testAFetchStringifiesADoubleAtTheRoundTripPrecision(string $class): void
    {
        $adapter = $this->adapter($class);
        $fetchExactly = (new \ReflectionMethod($adapter, 'fetchExactly'))->getClosure($adapter);
        $precision = \ini_get('precision');

        $this->assertSame('0.30000000000000004', $fetchExactly(static fn (): string => (string) (0.1 + 0.2)));
        $this->assertSame('5.0E-324', $fetchExactly(static fn (): string => (string) 5e-324));
        $this->assertSame($precision, \ini_get('precision'));

        try {
            $fetchExactly(static fn () => throw new \RuntimeException('fetch failed'));
            $this->fail('The fetch failure was swallowed');
        } catch (\RuntimeException) {
        }
        $this->assertSame($precision, \ini_get('precision'));
    }

    private function assertBound(float $value, string $bound): void
    {
        $this->assertContains($bound, $this->bindings);
        $this->assertSame($value, (float) $bound);
        foreach ($this->bindings as $binding) {
            $this->assertIsNotFloat($binding);
        }
    }

    private function collection(): Collection
    {
        return Collection::create(id: 'numbers', attributes: [Attribute::double(key: 'f')]);
    }

    /**
     * @param  class-string<SQL>  $class
     */
    private function adapter(string $class): SQL
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (): PDOStatement {
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('bindValue')->willReturnCallback(function (int|string $parameter, mixed $value): bool {
                $this->bindings[] = $value;

                return true;
            });
            $statement->method('fetchAll')->willReturn([]);
            $statement->method('fetch')->willReturn(false);
            $statement->method('fetchColumn')->willReturn(false);
            $statement->method('rowCount')->willReturn(1);
            $statement->method('closeCursor')->willReturn(true);

            return $statement;
        });
        $pdo->method('lastInsertId')->willReturn('1');
        $pdo->method('quote')->willReturnCallback(static fn (string $value): string => "'".$value."'");

        $adapter = new $class($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        return $adapter;
    }
}
